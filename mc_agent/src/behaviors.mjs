import fs from "node:fs";
import { randomUUID } from "node:crypto";
import mineflayer from "mineflayer";
import pathfinderPkg from "mineflayer-pathfinder";
import {
  ChatRingBuffer,
  ProtocolError,
  hasForbiddenTargetOverride,
  sanitizeMcText,
} from "./protocol.mjs";

const { Movements, goals } = pathfinderPkg;
const { GoalFollow, GoalNear, GoalNearXZ } = goals;

export const ALLOWED_GOAL_ACTIONS = new Set([
  "status",
  "connect",
  "disconnect",
  "goto",
  "explore",
  "mine",
  "gather",
  "follow",
  "look_signs",
  "observe",
  "build_plan",
  "chat",
  "stop",
  "idle",
]);

export const RARE_BLOCKS = new Set([
  "diamond_ore",
  "deepslate_diamond_ore",
  "ancient_debris",
  "emerald_ore",
  "spawner",
]);

const DEFAULT_MINE_BLOCKS = [
  "diamond_ore",
  "deepslate_diamond_ore",
  "iron_ore",
  "coal_ore",
  "stone",
];

const DEFAULT_GATHER_BLOCKS = [
  "oak_log",
  "birch_log",
  "spruce_log",
  "coal_ore",
  "cobblestone",
];

function roundCoord(v) {
  const num = Number(v);
  if (!Number.isFinite(num)) return 0;
  return Math.round(num * 10) / 10;
}

function readNonNegativeFinite(v) {
  if (typeof v !== "number" || !Number.isFinite(v)) return null;
  return Math.max(0, Math.round(v * 10) / 10);
}

function extractBuildTopic(params) {
  let raw = "";
  if (typeof params.topic === "string" && params.topic.trim()) {
    raw = params.topic;
  } else if (
    params.intent_note &&
    typeof params.intent_note === "object" &&
    typeof params.intent_note.note === "string"
  ) {
    raw = params.intent_note.note;
  } else if (typeof params.intent_note === "string") {
    raw = params.intent_note;
  }
  return sanitizeMcText(raw, 48) || "自由建造";
}

function extractCoords(params) {
  const src =
    params.x !== undefined || params.y !== undefined || params.z !== undefined
      ? params
      : params.args && typeof params.args === "object"
        ? params.args
        : params.payload && typeof params.payload === "object"
          ? params.payload
          : params;
  const x = Number(src.x);
  const y = Number(src.y);
  const z = Number(src.z);
  if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) {
    return null;
  }
  return { x, y, z };
}

export class McRuntime {
  constructor({
    config,
    createBotFn = mineflayer.createBot,
    onEvent = () => {},
    snapshotIntervalMs = 10_000,
    milestoneCooldownMs = 60_000,
    actionTimeoutMs = 25_000,
    chatCooldownMs = 3_000,
    profilesFolder = "/tmp/mc_cache",
    nowFn = () => Date.now(),
  }) {
    this.config = config;
    this.createBotFn = createBotFn;
    this.onEvent = onEvent;
    this.snapshotIntervalMs = snapshotIntervalMs;
    this.milestoneCooldownMs = milestoneCooldownMs;
    this.actionTimeoutMs = Math.min(30_000, Math.max(100, actionTimeoutMs));
    this.chatCooldownMs =
      typeof chatCooldownMs === "number" && Number.isFinite(chatCooldownMs)
        ? Math.max(0, chatCooldownMs)
        : 3_000;
    this.profilesFolder = profilesFolder;
    this.nowFn = nowFn;

    this.connected = false;
    this.paused = false;
    this.dimension = "";
    this.pos = null;
    this.health = null;
    this.food = null;
    this.activity = "待机";
    this.nearbyEntities = [];
    this.chatBuffer = new ChatRingBuffer(3, 120);
    this.updatedAt = this.nowFn() / 1000;

    this.bot = null;
    this._snapshotTimer = null;
    this._reconnectTimer = null;
    this._reconnectAttempts = 0;
    this._manualDisconnect = false;
    this._stopped = false;
    this._activeGoalCancel = null;
    this._prevHealth = null;
    this._lastChatAtMs = -Infinity;
    this._milestoneCooldowns = new Map();
  }

  start() {
    this._stopped = false;
    if (this.config.mcEnabled && this.config.mcConfigured && !this.paused) {
      this.connectBot();
    }
  }

  stop() {
    this._stopped = true;
    this._manualDisconnect = true;
    this._clearReconnectTimer();
    this._stopSnapshotTimer();
    this._cancelActiveGoal();
    this._teardownBot(true);
  }

  getHealthz() {
    return {
      status: "ok",
      bridge_ready: true,
      mc_enabled: this.config.mcEnabled,
      mc_configured: this.config.mcConfigured,
      mc_connected: this.connected,
      paused: this.paused,
    };
  }

  getSnapshot() {
    return {
      connected: this.connected,
      bridge_ready: true,
      mc_enabled: this.config.mcEnabled,
      mc_configured: this.config.mcConfigured,
      paused: this.paused,
      dimension: this.dimension,
      pos: this.pos ? [...this.pos] : null,
      health: this.health,
      food: this.food,
      active_task: this._activeGoalCancel && this.connected && !this.paused ? this.activeTask : "",
      activity: sanitizeMcText(this.activity, 64) || (this.connected ? "原地待命" : "待机"),
      nearby_entities: [...this.nearbyEntities],
      recent_chat_or_sign: this.chatBuffer.format(),
      updated_at: this.updatedAt,
    };
  }

  getStatusData() {
    const snap = this.getSnapshot();
    return {
      connected: snap.connected,
      paused: snap.paused,
      bridge_ready: true,
      mc_enabled: snap.mc_enabled,
      mc_configured: snap.mc_configured,
      activity: snap.activity,
      phase: "phase4_live",
      updated_at: snap.updated_at,
      snapshot: snap,
    };
  }

  _touch() {
    this.updatedAt = this.nowFn() / 1000;
  }

  _emitSnapshot() {
    this._touch();
    this.onEvent("mc.snapshot", this.getSnapshot());
  }

  _emitMilestone(eventType, importance, payload) {
    const nowMs = this.nowFn();
    const lastMs = this._milestoneCooldowns.get(eventType) ?? -Infinity;
    if (eventType !== "mc_goal_completed" && nowMs - lastMs < this.milestoneCooldownMs) {
      return false;
    }
    this._milestoneCooldowns.set(eventType, nowMs);
    this.onEvent("mc.milestone", {
      event_type: eventType,
      event_id: randomUUID(),
      importance,
      payload,
    });
    return true;
  }

  _vecToTuple(vec) {
    if (!vec || typeof vec !== "object") return null;
    const x = Number(vec.x);
    const y = Number(vec.y);
    const z = Number(vec.z);
    if (!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(z)) {
      return null;
    }
    return [roundCoord(x), roundCoord(y), roundCoord(z)];
  }

  _refreshFromBot() {
    const bot = this.bot;
    if (!bot || !this.connected) return;
    const newDim = sanitizeMcText(bot.game?.dimension || "", 24);
    const dimChanged = Boolean(this.dimension && newDim && newDim !== this.dimension);
    if (newDim) {
      this.dimension = newDim;
    }
    const nextPos = this._vecToTuple(bot.entity?.position);
    if (nextPos) {
      this.pos = nextPos;
    }
    const nextHealth = readNonNegativeFinite(bot.health);
    if (nextHealth !== null) {
      this.health = nextHealth;
    }
    const nextFood = readNonNegativeFinite(bot.food);
    if (nextFood !== null) {
      this.food = nextFood;
    }
    this.nearbyEntities = this._collectNearbyEntities(bot);
    if (dimChanged) {
      this._emitSnapshot();
    }
  }

  _collectNearbyEntities(bot) {
    const origin = bot.entity?.position;
    const rawEntities = Object.values(bot.entities || {});
    const withDist = [];
    for (const ent of rawEntities) {
      if (!ent || ent === bot.entity) continue;
      let dist = 0;
      if (origin && ent.position && typeof origin.distanceTo === "function") {
        dist = origin.distanceTo(ent.position);
        if (!Number.isFinite(dist) || dist > 24) continue;
      } else if (origin && ent.position) {
        const dx = (ent.position.x ?? 0) - (origin.x ?? 0);
        const dy = (ent.position.y ?? 0) - (origin.y ?? 0);
        const dz = (ent.position.z ?? 0) - (origin.z ?? 0);
        dist = Math.hypot(dx, dy, dz);
        if (!Number.isFinite(dist) || dist > 24) continue;
      }
      const rawName =
        ent.type === "player" && ent.username
          ? ent.username
          : ent.name || ent.displayName || ent.type || "";
      const cleanName = sanitizeMcText(rawName, 24);
      if (cleanName) {
        withDist.push({ dist, label: cleanName });
      }
    }
    withDist.sort((a, b) => a.dist - b.dist);
    return withDist.slice(0, 5).map((item) => item.label);
  }

  _startSnapshotTimer() {
    this._stopSnapshotTimer();
    if (!this.connected) return;
    this._snapshotTimer = setInterval(() => {
      if (!this.connected || this._stopped) {
        this._stopSnapshotTimer();
        return;
      }
      this._refreshFromBot();
      this._emitSnapshot();
    }, this.snapshotIntervalMs);
    if (typeof this._snapshotTimer.unref === "function") {
      this._snapshotTimer.unref();
    }
  }

  _stopSnapshotTimer() {
    if (this._snapshotTimer) {
      clearInterval(this._snapshotTimer);
      this._snapshotTimer = null;
    }
  }

  _clearReconnectTimer() {
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
  }

  _scheduleReconnect() {
    this._clearReconnectTimer();
    if (
      this._stopped ||
      this._manualDisconnect ||
      this.paused ||
      !this.config.mcEnabled ||
      !this.config.mcConfigured
    ) {
      return;
    }
    const baseMs = Math.min(60_000, 5000 * 2 ** Math.min(this._reconnectAttempts, 4));
    this._reconnectAttempts += 1;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      if (!this._stopped && !this._manualDisconnect && !this.paused) {
        this.connectBot();
      }
    }, baseMs);
    if (typeof this._reconnectTimer.unref === "function") {
      this._reconnectTimer.unref();
    }
  }

  _cancelActiveGoal() {
    if (this._activeGoalCancel) {
      const fn = this._activeGoalCancel;
      this._activeGoalCancel = null;
      fn();
    }
  }

  _ensurePathfinder(bot) {
    if (!bot.pathfinder && typeof bot.loadPlugin === "function" && bot.registry?.blocksByName) {
      bot.loadPlugin(pathfinderPkg.pathfinder);
    }
    if (
      bot.pathfinder &&
      typeof bot.pathfinder.setMovements === "function" &&
      bot.registry?.blocksByName
    ) {
      const movements = new Movements(bot);
      bot.pathfinder.setMovements(movements);
    }
  }

  connectBot() {
    if (this.bot || this._stopped) return;
    if (!this.config.mcEnabled || !this.config.mcConfigured) return;

    this._manualDisconnect = false;
    this._clearReconnectTimer();
    if (!this.activity.startsWith("筹备建造:")) {
      this.activity = "连接中";
    }

    if (this.config.mcAuth === "microsoft") {
      fs.mkdirSync(this.profilesFolder, { recursive: true, mode: 0o700 });
    }

    const opts = {
      host: this.config.mcHost,
      port: this.config.mcPort,
      username: this.config.mcUsername,
      auth: this.config.mcAuth,
      viewDistance: this.config.mcViewDistance,
      hideErrors: true,
      logErrors: false,
      profilesFolder: this.config.mcAuth === "microsoft" ? this.profilesFolder : false,
    };
    if (this.config.mcVersion) {
      opts.version = this.config.mcVersion;
    }

    let bot;
    try {
      bot = this.createBotFn(opts);
    } catch {
      this._scheduleReconnect();
      return;
    }
    this.bot = bot;

    bot.on("inject_allowed", () => {
      if (this.bot !== bot) return;
      this._ensurePathfinder(bot);
    });

    bot.on("spawn", () => {
      if (this.bot !== bot) return;
      this._ensurePathfinder(bot);
      this.connected = true;
      this._reconnectAttempts = 0;
      this.dimension = sanitizeMcText(bot.game?.dimension || "overworld", 24) || "overworld";
      this.pos = this._vecToTuple(bot.entity?.position);
      this.health = readNonNegativeFinite(bot.health);
      this.food = readNonNegativeFinite(bot.food);
      this._prevHealth = this.health;
      this.nearbyEntities = this._collectNearbyEntities(bot);
      if (
        !this.activity ||
        this.activity === "待机" ||
        this.activity === "连接中" ||
        this.activity === "连接已断开" ||
        this.activity === "等待复活"
      ) {
        this.activity = this.paused ? "已暂停" : "原地待命";
      }
      this._startSnapshotTimer();
      this._emitSnapshot();
    });

    bot.on("respawn", () => {
      if (this.bot !== bot) return;
      const newDim = sanitizeMcText(bot.game?.dimension || "", 24);
      const changed = newDim && newDim !== this.dimension;
      if (newDim) this.dimension = newDim;
      this.pos = this._vecToTuple(bot.entity?.position);
      this.health = readNonNegativeFinite(bot.health);
      this.food = readNonNegativeFinite(bot.food);
      this._prevHealth = this.health;
      this.nearbyEntities = this._collectNearbyEntities(bot);
      if (this.activity === "等待复活") {
        this.activity = this.paused ? "已暂停" : "原地待命";
      }
      if (changed || this.connected) {
        this._emitSnapshot();
      }
    });

    bot.on("health", () => {
      if (this.bot !== bot || !this.connected) return;
      const newHealth = readNonNegativeFinite(bot.health);
      const prev = this._prevHealth;
      this._refreshFromBot();
      if (
        newHealth !== null &&
        prev !== null &&
        newHealth > 0 &&
        newHealth <= 6 &&
        newHealth < prev
      ) {
        this._emitMilestone("mc_health_alert", "high", {
          health: newHealth,
          dimension: this.dimension || "overworld",
          pos: this.pos,
        });
        this._emitSnapshot();
      }
      this._prevHealth = newHealth;
    });

    bot.on("death", () => {
      if (this.bot !== bot) return;
      this._cancelActiveGoal();
      const prevActivity = sanitizeMcText(this.activity, 48) || "探索";
      this._refreshFromBot();
      this.health = 0;
      this._prevHealth = 0;
      this.activity = "等待复活";
      this._emitMilestone("mc_death", "high", {
        dimension: this.dimension || "overworld",
        pos: this.pos,
        activity: prevActivity,
      });
      this._emitSnapshot();
    });

    bot.on("chat", (username, message) => {
      if (this.bot !== bot) return;
      if (username === bot.username) return;
      const cleanUser = sanitizeMcText(username, 40);
      const cleanMsg = sanitizeMcText(message, 80);
      if (!cleanUser && !cleanMsg) return;
      this.chatBuffer.push(cleanUser ? `${cleanUser}: ${cleanMsg}` : cleanMsg);
    });

    bot.on("entitySpawn", (entity) => {
      if (this.bot !== bot || !this.connected) return;
      if (
        !entity ||
        entity === bot.entity ||
        entity.type !== "player" ||
        !entity.username ||
        entity.username === bot.username
      ) {
        return;
      }
      const cleanName = sanitizeMcText(entity.username, 40);
      if (!cleanName) return;
      this._emitMilestone("mc_player_spotted", "normal", {
        player: cleanName,
        pos: this._vecToTuple(entity.position),
      });
    });

    bot.on("kicked", () => {
      if (this.bot !== bot) return;
      this._handleBotEnd(bot);
    });

    bot.on("error", () => {
      if (this.bot !== bot) return;
      if (!this.connected) {
        this._handleBotEnd(bot);
      }
    });

    bot.on("end", () => {
      if (this.bot !== bot) return;
      this._handleBotEnd(bot);
    });
  }

  _handleBotEnd(bot) {
    if (this.bot !== bot) return;
    const wasConnected = this.connected;
    this._stopSnapshotTimer();
    this._cancelActiveGoal();
    this.bot = null;
    this.connected = false;
    this.dimension = "";
    this.pos = null;
    this.health = null;
    this.food = null;
    this.nearbyEntities = [];
    this._prevHealth = null;
    if (!this.activity.startsWith("筹备建造:")) {
      this.activity = this._manualDisconnect ? "待机" : "连接已断开";
    }
    if (wasConnected || this._manualDisconnect) {
      this._emitSnapshot();
    }
    this._scheduleReconnect();
  }

  _teardownBot(intentional) {
    const bot = this.bot;
    if (!bot) return;
    this.bot = null;
    this._stopSnapshotTimer();
    this._cancelActiveGoal();
    const wasConnected = this.connected;
    this.connected = false;
    this.dimension = "";
    this.pos = null;
    this.health = null;
    this.food = null;
    this.nearbyEntities = [];
    this._prevHealth = null;
    if (intentional && !this.activity.startsWith("筹备建造:")) {
      this.activity = "待机";
    }
    try {
      if (typeof bot.quit === "function") {
        bot.quit();
      } else if (typeof bot.end === "function") {
        bot.end();
      }
    } catch {
      // Ignore socket teardown errors
    }
    if (wasConnected) {
      this._emitSnapshot();
    }
  }

  async _runBoundedGoal(actionType, timeoutMs, fn) {
    this._cancelActiveGoal();
    const boundedMs = Math.min(30_000, Math.max(100, timeoutMs ?? this.actionTimeoutMs));
    const controller = new AbortController();
    let timer = null;
    let timedOut = false;

    const cancel = () => {
      if (!controller.signal.aborted) {
        controller.abort();
      }
      try {
        this.bot?.pathfinder?.stop?.();
        this.bot?.pathfinder?.setGoal?.(null);
      } catch {
        // Ignore pathfinder stop errors
      }
    };
    this.activeTask = actionType;
    this._activeGoalCancel = cancel;
    this._emitSnapshot();

    try {
      const timeoutPromise = new Promise((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          cancel();
          reject(new Error("mc_action_timeout"));
        }, boundedMs);
      });
      const res = await Promise.race([fn(controller.signal), timeoutPromise]);
      if (timedOut) return { timedOut: true };
      if (controller.signal.aborted) return { aborted: true };
      return res;
    } catch (err) {
      if (timedOut) {
        return { timedOut: true };
      }
      if (controller.signal.aborted) {
        return { aborted: true };
      }
      throw err;
    } finally {
      if (timer) clearTimeout(timer);
      if (this._activeGoalCancel === cancel) {
        this._activeGoalCancel = null;
        this._emitSnapshot();
      }
    }
  }

  async _gotoGoal(goal, signal) {
    const bot = this.bot;
    if (!bot?.pathfinder || typeof bot.pathfinder.goto !== "function") {
      throw new Error("pathfinder_unavailable");
    }
    this._ensurePathfinder(bot);
    await bot.pathfinder.goto(goal).catch((err) => {
      if (signal.aborted) return;
      throw err;
    });
  }

  _scanRareBlocks(bot, maxDistance = 24) {
    if (typeof bot.findBlocks !== "function") return [];
    const positions =
      bot.findBlocks({
        matching: (b) => Boolean(b && RARE_BLOCKS.has(b.name)),
        point: bot.entity?.position,
        maxDistance,
        count: 4,
      }) || [];
    const found = [];
    for (const p of positions) {
      const block = typeof bot.blockAt === "function" ? bot.blockAt(p) : null;
      if (!block || !RARE_BLOCKS.has(block.name)) continue;
      const coords = this._vecToTuple(p) || [0, 0, 0];
      found.push({ block: block.name, pos: coords });
      this._emitMilestone("mc_rare_find", "high", {
        block: block.name,
        pos: coords,
        dimension: this.dimension || "overworld",
      });
    }
    return found;
  }

  _scanSigns(bot, maxDistance = 24) {
    if (typeof bot.findBlocks !== "function") return [];
    const positions =
      bot.findBlocks({
        matching: (b) => Boolean(b && typeof b.name === "string" && b.name.includes("sign")),
        point: bot.entity?.position,
        maxDistance,
        count: 8,
      }) || [];
    const signs = [];
    for (const p of positions) {
      const block = typeof bot.blockAt === "function" ? bot.blockAt(p) : null;
      if (!block) continue;
      let rawLines = [];
      if (typeof block.getSignText === "function") {
        const res = block.getSignText();
        rawLines = Array.isArray(res) ? res : [res];
      } else if (typeof block.signText === "string") {
        rawLines = [block.signText];
      }
      const clean = sanitizeMcText(rawLines.filter(Boolean).join(" "), 80);
      if (clean) {
        signs.push(clean);
        this.chatBuffer.push(`告示牌:${clean}`);
      }
    }
    return signs;
  }

  async pause() {
    this._cancelActiveGoal();
    this.paused = true;
    this.activity = "已暂停";
    this._emitSnapshot();
    const snap = this.getSnapshot();
    return {
      ok: true,
      connected: snap.connected,
      paused: true,
      bridge_ready: true,
      snapshot: snap,
    };
  }

  async resume() {
    this.paused = false;
    if (this.activity === "已暂停") {
      this.activity = this.connected ? "原地待命" : "待机";
    }
    this._emitSnapshot();
    const snap = this.getSnapshot();
    return {
      ok: true,
      connected: snap.connected,
      paused: false,
      bridge_ready: true,
      snapshot: snap,
    };
  }

  async handleRpc(action, params) {
    const safeParams = params ?? {};
    if (typeof safeParams !== "object" || Array.isArray(safeParams)) {
      throw new ProtocolError("invalid_params");
    }

    if (action === "mc.status") {
      this._refreshFromBot();
      return this.getStatusData();
    }
    if (action === "mc.pause") {
      return await this.pause();
    }
    if (action === "mc.resume") {
      return await this.resume();
    }
    if (action === "mc.dispatch_goal") {
      return await this.dispatchGoal(safeParams);
    }
    throw new ProtocolError("unsupported_rpc_action");
  }

  async dispatchGoal(params) {
    if (hasForbiddenTargetOverride(params)) {
      throw new ProtocolError("mc_target_override_forbidden");
    }

    const rawAction = params.action_type ?? params.goal ?? "status";
    const actionType = typeof rawAction === "string" ? rawAction.trim().toLowerCase() : "";
    if (!ALLOWED_GOAL_ACTIONS.has(actionType)) {
      throw new ProtocolError("unsupported_mc_action");
    }

    if (this.paused && actionType !== "status" && actionType !== "stop" && actionType !== "idle") {
      return {
        ok: false,
        accepted: false,
        bridge_ready: true,
        paused: true,
        reason: "mc_paused",
        summary: "MC躯干已暂停",
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "status") {
      this._refreshFromBot();
      return {
        ok: true,
        accepted: true,
        ...this.getStatusData(),
      };
    }

    if (actionType === "build_plan") {
      const topic = extractBuildTopic(params);
      this.activity = `筹备建造:${topic}`;
      this._emitSnapshot();
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary: this.activity,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "idle" || actionType === "stop") {
      this._cancelActiveGoal();
      this.activity = this.connected ? "原地待命" : "待机";
      this._emitSnapshot();
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: this.paused,
        summary: this.activity,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "connect") {
      if (!this.config.mcEnabled || !this.config.mcConfigured) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: !this.config.mcEnabled ? "mc_disabled" : "mc_host_not_configured",
          summary: "MC服务端未启用或未配置",
          snapshot: this.getSnapshot(),
        };
      }
      if (this.connected) {
        return {
          ok: true,
          accepted: true,
          bridge_ready: true,
          paused: false,
          summary: "MC已连接",
          snapshot: this.getSnapshot(),
        };
      }
      this.connectBot();
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary: "正在连接MC服务端",
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "disconnect") {
      this._manualDisconnect = true;
      this._clearReconnectTimer();
      this._teardownBot(true);
      this.activity = "待机";
      this._emitSnapshot();
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: this.paused,
        summary: "已断开MC服务端",
        snapshot: this.getSnapshot(),
      };
    }

    if (!this.connected || !this.bot) {
      return {
        ok: false,
        accepted: false,
        bridge_ready: true,
        paused: false,
        reason: "mc_server_not_connected",
        summary: "MC躯干就绪（当前未连接游戏服）",
        snapshot: this.getSnapshot(),
      };
    }

    return await this._executeInGameGoal(actionType, params);
  }

  async _executeInGameGoal(actionType, params) {
    const bot = this.bot;
    const timeoutMs =
      typeof params.timeout_ms === "number" && Number.isFinite(params.timeout_ms)
        ? Math.min(30_000, Math.max(100, params.timeout_ms))
        : this.actionTimeoutMs;

    if (actionType === "goto") {
      const coords = extractCoords(params);
      if (!coords) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "invalid_coordinates",
          summary: "坐标参数无效",
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = `前往(${Math.round(coords.x)},${Math.round(coords.y)},${Math.round(coords.z)})`;
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        await this._gotoGoal(new GoalNear(coords.x, coords.y, coords.z, 1), signal);
        return { ok: true };
      });
      this._refreshFromBot();
      if (outcome?.timedOut || outcome?.aborted) {
        this.activity = this.paused ? "已暂停" : "原地待命";
        this._emitSnapshot();
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "寻路超时" : "寻路已取消",
          snapshot: this.getSnapshot(),
        };
      }
      const summary = `已到达(${Math.round(coords.x)},${Math.round(coords.y)},${Math.round(coords.z)})附近`;
      this.activity = this.paused ? "已暂停" : "原地待命";
      this._emitSnapshot();
      this._emitMilestone("mc_goal_completed", "normal", { action_type: "goto", summary });
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "explore") {
      const cur = this.pos || [0, 64, 0];
      const dx = (Math.random() < 0.5 ? -1 : 1) * (12 + Math.floor(Math.random() * 12));
      const dz = (Math.random() < 0.5 ? -1 : 1) * (12 + Math.floor(Math.random() * 12));
      const targetX = Math.round(cur[0] + dx);
      const targetZ = Math.round(cur[2] + dz);
      this.activity = `探索(${targetX},${targetZ})`;
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        await this._gotoGoal(new GoalNearXZ(targetX, targetZ, 2), signal);
        return { ok: true };
      });
      this._refreshFromBot();
      const rare = this._scanRareBlocks(bot, 24);
      const signs = this._scanSigns(bot, 24);
      this.activity = this.paused ? "已暂停" : "原地待命";
      this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "探索超时" : "探索已取消",
          snapshot: this.getSnapshot(),
        };
      }
      const summary = `探索至(${targetX},${targetZ}) 发现稀有矿${rare.length}处 告示牌${signs.length}块`;
      this._emitMilestone("mc_goal_completed", "normal", { action_type: "explore", summary });
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary,
        rare_blocks: rare,
        signs,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "mine" || actionType === "gather") {
      const rawBlock = sanitizeMcText(params.block_type || params.block || params.topic || "", 48)
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, "");
      const targets = new Set(
        rawBlock ? [rawBlock] : actionType === "gather" ? DEFAULT_GATHER_BLOCKS : DEFAULT_MINE_BLOCKS,
      );
      this.activity = rawBlock ? `采集:${rawBlock}` : actionType === "gather" ? "采集资源" : "采矿中";
      this._emitSnapshot();

      const positions =
        typeof bot.findBlocks === "function"
          ? bot.findBlocks({
              matching: (b) => Boolean(b && targets.has(b.name)),
              point: bot.entity?.position,
              maxDistance: 32,
              count: 4,
            }) || []
          : [];

      if (positions.length === 0) {
        this._refreshFromBot();
        this.activity = this.paused ? "已暂停" : "原地待命";
        this._emitSnapshot();
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: "block_not_found",
          summary: `附近未找到${rawBlock || "目标方块"}`,
          snapshot: this.getSnapshot(),
        };
      }

      const targetPos = positions[0];
      const block = typeof bot.blockAt === "function" ? bot.blockAt(targetPos) : null;
      const blockName = sanitizeMcText(block?.name || rawBlock || "block", 48);
      const coords = this._vecToTuple(targetPos) || [0, 0, 0];

      if (RARE_BLOCKS.has(blockName)) {
        this._emitMilestone("mc_rare_find", "high", {
          block: blockName,
          pos: coords,
          dimension: this.dimension || "overworld",
        });
      }

      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        if (bot.pathfinder?.goto) {
          await this._gotoGoal(new GoalNear(coords[0], coords[1], coords[2], 3), signal);
        }
        if (signal.aborted) return { aborted: true };
        if (block && typeof bot.dig === "function") {
          await bot.dig(block);
        }
        return { ok: true };
      });

      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : "原地待命";
      this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "采集超时" : "采集已取消",
          snapshot: this.getSnapshot(),
        };
      }
      const summary = `已采集${blockName}(${coords.join(",")})`;
      this._emitMilestone("mc_goal_completed", "normal", { action_type: actionType, summary });
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary,
        block: blockName,
        pos: coords,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "follow") {
      const targetPlayer = sanitizeMcText(
        params.target_player || params.player || params.topic || "",
        32,
      );
      if (
        !targetPlayer ||
        this.config.mcAllowedPlayers.size === 0 ||
        !this.config.mcAllowedPlayers.has(targetPlayer.toLowerCase())
      ) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "player_not_allowed",
          summary: "目标玩家不在允许列表",
          snapshot: this.getSnapshot(),
        };
      }
      const players = Object.values(bot.players || {});
      const matched = players.find(
        (p) =>
          p &&
          typeof p.username === "string" &&
          p.username.toLowerCase() === targetPlayer.toLowerCase(),
      );
      if (!matched?.entity) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: "player_not_visible",
          summary: `未在视野内发现${targetPlayer}`,
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = `跟随:${sanitizeMcText(matched.username, 24)}`;
      this._emitSnapshot();
      const followTimeout = Math.min(15_000, timeoutMs);
      const outcome = await this._runBoundedGoal(actionType, followTimeout, async (signal) => {
        await this._gotoGoal(new GoalFollow(matched.entity, 2), signal);
        return { ok: true };
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : "原地待命";
      this._emitSnapshot();
      if (outcome?.aborted && !outcome?.timedOut) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: "mc_goal_cancelled",
          summary: "跟随已取消",
          snapshot: this.getSnapshot(),
        };
      }
      const summary = `已跟随${sanitizeMcText(matched.username, 24)}`;
      this._emitMilestone("mc_goal_completed", "normal", { action_type: "follow", summary });
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "look_signs" || actionType === "observe") {
      this.activity = actionType === "look_signs" ? "查看告示牌" : "观察周围";
      const signs = this._scanSigns(bot, 24);
      const rare = this._scanRareBlocks(bot, 24);
      this._refreshFromBot();
      for (const p of Object.values(bot.players || {})) {
        if (p?.entity && p.username && p.username !== bot.username) {
          this._emitMilestone("mc_player_spotted", "normal", {
            player: sanitizeMcText(p.username, 24),
            pos: this._vecToTuple(p.entity.position),
          });
        }
      }
      this.activity = this.paused ? "已暂停" : "原地待命";
      this._emitSnapshot();
      const summary =
        signs.length > 0
          ? `查看${signs.length}块告示牌: ${signs[0].slice(0, 40)}`
          : `观察周围 发现实体${this.nearbyEntities.length}个 稀有矿${rare.length}处`;
      this._emitMilestone("mc_goal_completed", "normal", { action_type: actionType, summary });
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary,
        signs,
        rare_blocks: rare,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "chat") {
      if (!this.config.mcAllowChat) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "mc_chat_disabled",
          summary: "MC公屏聊天未开启",
          snapshot: this.getSnapshot(),
        };
      }
      const rawText = String(params.message ?? params.text ?? params.topic ?? "");
      if (rawText.trim().startsWith("/")) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "mc_command_forbidden",
          summary: "禁止在公屏发送斜杠指令",
          snapshot: this.getSnapshot(),
        };
      }
      const cleanMsg = sanitizeMcText(rawText, 120);
      if (!cleanMsg) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "empty_chat_message",
          summary: "公屏消息为空",
          snapshot: this.getSnapshot(),
        };
      }
      if (cleanMsg.startsWith("/")) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "mc_command_forbidden",
          summary: "禁止在公屏发送斜杠指令",
          snapshot: this.getSnapshot(),
        };
      }
      const nowMs = this.nowFn();
      if (this.chatCooldownMs > 0 && nowMs - this._lastChatAtMs < this.chatCooldownMs) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "mc_chat_rate_limited",
          summary: "公屏发言冷却中",
          snapshot: this.getSnapshot(),
        };
      }
      if (typeof bot.chat !== "function") {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: "mc_chat_unavailable",
          summary: "当前连接不支持公屏发言",
          snapshot: this.getSnapshot(),
        };
      }
      bot.chat(cleanMsg);
      this._lastChatAtMs = nowMs;
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : "原地待命";
      this._emitSnapshot();
      const summary = "公屏发言: " + cleanMsg.slice(0, 40);
      this._emitMilestone("mc_goal_completed", "normal", { action_type: "chat", summary });
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary,
        message: cleanMsg,
        snapshot: this.getSnapshot(),
      };
    }

    throw new ProtocolError("unsupported_mc_action");
  }
}
