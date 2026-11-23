import fs from "node:fs";
import { randomUUID } from "node:crypto";
import mineflayer from "mineflayer";
import pathfinderPkg from "mineflayer-pathfinder";
import {
  ChatRingBuffer,
  ProtocolError,
  hasForbiddenTargetOverride,
  normalizePlayerId,
  sanitizeMcText,
} from "./protocol.mjs";
import {
  LOW_FOOD_THRESHOLD,
  autoEquipBestGear,
  collectInventorySnapshot,
  countSafeFood,
  createEmptyEquipment,
  equipBestHarvestTool,
  performAutoEat,
  shouldTriggerAutoEat,
} from "./survival.mjs";
import { executeCraftAction, parseCraftParams } from "./crafting.mjs";
import { executeSmeltAction, parseSmeltParams } from "./smelting.mjs";
import {
  applySafeMovements,
  checkHarvestToolTier,
  executeChestAction,
  executeGiveAction,
  executeHuntAction,
  executePlaceAction,
  executeSleepAction,
  hasExposedFace,
  isBedName,
  parseChestParams,
  parseGiveParams,
  parseHuntParams,
  parsePlaceParams,
} from "./world_actions.mjs";
import {
  CREEPER_SAFE_DISTANCE,
  HOSTILE_MOBS,
  MELEE_EVADE_RADIUS,
  MELEE_FLEE_DISTANCE,
  RANGED_EVADE_RADIUS,
  RANGED_FLEE_DISTANCE,
  RANGED_HOSTILE_MOBS,
  applyNoDigMovements,
  computeRetreatTarget,
  createCombatState,
  findNearestCreeper,
  lowerShield,
  runGuardCombatStep,
} from "./combat.mjs";

export { HOSTILE_MOBS };

const { goals } = pathfinderPkg;
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
  "craft",
  "smelt",
  "guard",
  "evade",
  "hunt",
  "sleep",
  "chest",
  "place",
  "give",
]);

export const NEARBY_RESOURCE_BLOCKS = new Set([
  "oak_log",
  "spruce_log",
  "birch_log",
  "jungle_log",
  "acacia_log",
  "dark_oak_log",
  "mangrove_log",
  "cherry_log",
  "pale_oak_log",
  "stone",
  "cobblestone",
  "coal_ore",
  "deepslate_coal_ore",
  "iron_ore",
  "deepslate_iron_ore",
  "copper_ore",
  "deepslate_copper_ore",
  "gold_ore",
  "deepslate_gold_ore",
  "diamond_ore",
  "deepslate_diamond_ore",
  "crafting_table",
  "furnace",
  "smoker",
  "blast_furnace",
  "chest",
  "trapped_chest",
  "barrel",
  "hay_block",
  "wheat",
  "pumpkin",
  "melon",
  "sugar_cane",
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

const STAY_CMD_RE = /停下|别动|原地|等我|待着|\bstay\b|\bstop\b/i;
const FOLLOW_CMD_RE = /跟上|过来|跟我走|跟我来|陪我|走吧|\bfollow\b|\bcome\b/i;
const GUARD_CMD_RE = /护卫|保护我|打怪|帮我打|警戒|战斗模式|\bguard\b/i;
const EVADE_CMD_RE = /躲避|别打|撤退|避战|躲怪|逃跑模式|\bevade\b/i;
const GREET_CMD_RE = /宝宝|哈基米|猫猫|geminy|吉米奈|大小姐|在吗|你好|\bhi\b|\bhello\b/i;

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
    fetchFn = globalThis.fetch,
    powerPollIntervalMs = 2_000,
    powerWakeTimeoutMs = 90_000,
    idleDisconnectMs = undefined,
    soloMaxSessionMs = undefined,
    companionTickMs = 1_500,
    survivalTickMs = 1_500,
    eatTimeoutMs = 4_000,
    lowFoodCooldownMs = 180_000,
    soloWanderIntervalMs = 25_000,
    companionReplyCooldownMs = 5_000,
    hostileAlertCooldownMs = 60_000,
    pickupSettleMs = 120,
    smeltPollIntervalMs = 350,
    stuckCheckIntervalMs = 2_500,
    huntAttackCooldownMs = null,
  }) {
    this.config = config;
    this.createBotFn = createBotFn;
    this.onEvent = onEvent;
    this.snapshotIntervalMs = snapshotIntervalMs;
    this.milestoneCooldownMs = milestoneCooldownMs;
    this.actionTimeoutMs = Math.min(35_000, Math.max(100, actionTimeoutMs));
    this.pickupSettleMs =
      typeof pickupSettleMs === "number" && Number.isFinite(pickupSettleMs)
        ? Math.max(0, pickupSettleMs)
        : 120;
    this.smeltPollIntervalMs =
      typeof smeltPollIntervalMs === "number" && Number.isFinite(smeltPollIntervalMs)
        ? Math.max(10, smeltPollIntervalMs)
        : 350;
    this.stuckCheckIntervalMs =
      typeof stuckCheckIntervalMs === "number" && Number.isFinite(stuckCheckIntervalMs)
        ? Math.max(0, stuckCheckIntervalMs)
        : 2_500;
    this.huntAttackCooldownMs =
      typeof huntAttackCooldownMs === "number" && Number.isFinite(huntAttackCooldownMs) && huntAttackCooldownMs > 0
        ? huntAttackCooldownMs
        : null;
    this.chatCooldownMs =
      typeof chatCooldownMs === "number" && Number.isFinite(chatCooldownMs)
        ? Math.max(0, chatCooldownMs)
        : 3_000;
    this.profilesFolder = profilesFolder;
    this.nowFn = nowFn;
    this.fetchFn = fetchFn;
    this.powerPollIntervalMs =
      typeof powerPollIntervalMs === "number" && Number.isFinite(powerPollIntervalMs)
        ? Math.max(10, powerPollIntervalMs)
        : 2_000;
    this.powerWakeTimeoutMs =
      typeof powerWakeTimeoutMs === "number" && Number.isFinite(powerWakeTimeoutMs)
        ? Math.max(50, powerWakeTimeoutMs)
        : 90_000;
    const defaultIdleMinutes =
      typeof config.mcIdleDisconnectMinutes === "number" &&
      Number.isFinite(config.mcIdleDisconnectMinutes)
        ? config.mcIdleDisconnectMinutes
        : 20;
    this.idleDisconnectMs =
      typeof idleDisconnectMs === "number" && Number.isFinite(idleDisconnectMs)
        ? Math.max(0, idleDisconnectMs)
        : defaultIdleMinutes * 60_000;
    const defaultSoloMaxMinutes =
      typeof config.mcSoloMaxMinutes === "number" &&
      Number.isFinite(config.mcSoloMaxMinutes)
        ? config.mcSoloMaxMinutes
        : 90;
    this.soloMaxSessionMs =
      typeof soloMaxSessionMs === "number" && Number.isFinite(soloMaxSessionMs)
        ? Math.max(0, soloMaxSessionMs)
        : defaultSoloMaxMinutes * 60_000;

    this.connected = false;
    this.paused = false;
    this.dimension = "";
    this.pos = null;
    this.health = null;
    this.food = null;
    this.equipment = createEmptyEquipment();
    this.inventorySummary = [];
    this.freeSlots = null;
    this.combatMode = config.mcCombatMode === "guard" ? "guard" : "evade";
    this.guardTarget = "";
    this.timeOfDay = "";
    this.nearbyResources = [];
    this.activity = "待机";
    this.nearbyEntities = [];
    this.chatBuffer = new ChatRingBuffer(3, 120);
    this.updatedAt = this.nowFn() / 1000;

    this.bot = null;
    this._snapshotTimer = null;
    this._survivalTimer = null;
    this._combatMicroTimer = null;
    this._reconnectTimer = null;
    this._reconnectAttempts = 0;
    this._manualDisconnect = false;
    this._stopped = false;
    this._desiredConnected = false;
    this._wakeController = null;
    this._lastActivityOrHumanAtMs = this.nowFn();
    this._soloSinceMs = null;
    this._consecutiveReconnectFails = 0;
    this._activeGoalCancel = null;
    this._prevHealth = null;
    this._lastChatAtMs = -Infinity;
    this._milestoneCooldowns = new Map();
    this.companionTickMs =
      typeof companionTickMs === "number" && Number.isFinite(companionTickMs)
        ? Math.max(25, companionTickMs)
        : 1_500;
    this.survivalTickMs =
      typeof survivalTickMs === "number" && Number.isFinite(survivalTickMs)
        ? Math.max(25, survivalTickMs)
        : 1_500;
    this.eatTimeoutMs =
      typeof eatTimeoutMs === "number" && Number.isFinite(eatTimeoutMs)
        ? Math.max(50, eatTimeoutMs)
        : 4_000;
    this.lowFoodCooldownMs =
      typeof lowFoodCooldownMs === "number" && Number.isFinite(lowFoodCooldownMs)
        ? Math.max(0, lowFoodCooldownMs)
        : 180_000;
    this.soloWanderIntervalMs =
      typeof soloWanderIntervalMs === "number" && Number.isFinite(soloWanderIntervalMs)
        ? Math.max(50, soloWanderIntervalMs)
        : 25_000;
    this.companionReplyCooldownMs =
      typeof companionReplyCooldownMs === "number" && Number.isFinite(companionReplyCooldownMs)
        ? Math.max(0, companionReplyCooldownMs)
        : 5_000;
    this.hostileAlertCooldownMs =
      typeof hostileAlertCooldownMs === "number" && Number.isFinite(hostileAlertCooldownMs)
        ? Math.max(0, hostileAlertCooldownMs)
        : 60_000;
    this._companionTimer = null;
    this._stayPut = false;
    this._followingPlayer = null;
    this._greetedPlayers = new Set();
    this._lastSoloWanderAtMs = 0;
    this._lastAutoReplyAtMs = -Infinity;
    this._lastHostileAlertAtMs = -Infinity;
    this._lastLowFoodAtMs = -Infinity;
    this._eatingLock = { eating: false };
    this._combatState = createCombatState();
    this._survivalTickRunning = false;
    this._companionTickRunning = false;
  }

  get _eating() {
    return this._eatingLock.eating;
  }

  set _eating(val) {
    this._eatingLock.eating = Boolean(val);
  }

  start() {
    this._stopped = false;
    const autoConnect = this.config.mcAutoConnect !== false;
    if (autoConnect && this.config.mcEnabled && this.config.mcConfigured && !this.paused) {
      this._desiredConnected = true;
      this._consecutiveReconnectFails = 0;
      this.connectBot();
    } else {
      this._desiredConnected = false;
      if (!this.activity.startsWith("筹备建造:")) {
        this.activity = this.paused ? "已暂停" : "待机";
      }
    }
  }

  stop() {
    this._stopped = true;
    this._desiredConnected = false;
    this._manualDisconnect = true;
    this._soloSinceMs = null;
    this._cancelWakePoll();
    this._clearReconnectTimer();
    this._stopSurvivalLoop();
    this._stopCompanionLoop();
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

  _resolveGuardTargetName() {
    if (this.combatMode !== "guard" || !this.connected) return "";
    if (this.guardTarget) {
      return normalizePlayerId(this.guardTarget);
    }
    const nearest = this.bot ? this._findNearestAllowedPlayer(this.bot) : null;
    return nearest ? normalizePlayerId(nearest.player.username) : "";
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
      equipment: this.connected ? { ...this.equipment } : createEmptyEquipment(),
      inventory_summary: this.connected ? [...this.inventorySummary] : [],
      free_slots: this.connected ? this.freeSlots : null,
      combat_mode: this.combatMode,
      guard_target: this._resolveGuardTargetName(),
      time_of_day: this.connected ? this.timeOfDay : "",
      nearby_resources: this.connected ? [...this.nearbyResources] : [],
      active_task: this._activeGoalCancel && this.connected && !this.paused ? this.activeTask : "",
      activity: sanitizeMcText(this.activity, 64) || (this.connected ? "原地待命" : "待机"),
      nearby_entities: [...this.nearbyEntities],
      nearby_players: this._collectNearbyPlayers(),
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
    const invSnap = collectInventorySnapshot(bot);
    this.equipment = invSnap.equipment;
    this.inventorySummary = invSnap.inventory_summary;
    this.freeSlots = invSnap.free_slots;
    this.timeOfDay = this._computeTimeOfDay(bot);
    this.nearbyResources = this._collectNearbyResources(bot, 16);
    this.nearbyEntities = this._collectNearbyEntities(bot);
    if (this._hasOtherPlayer(bot)) {
      this._lastActivityOrHumanAtMs = this.nowFn();
    }
    this._syncSoloClock(bot);
    if (dimChanged) {
      this._emitSnapshot();
    }
  }

  _syncSoloClock(bot = this.bot) {
    if (!bot || !this.connected) {
      this._soloSinceMs = null;
      return;
    }
    if (this._hasOtherPlayer(bot)) {
      this._soloSinceMs = null;
    } else if (this._soloSinceMs === null) {
      this._soloSinceMs = this.nowFn();
    }
  }

  _hasOtherPlayer(bot) {
    if (!bot || !bot.players || typeof bot.players !== "object") return false;
    for (const p of Object.values(bot.players)) {
      if (p && typeof p.username === "string" && p.username && p.username !== bot.username) {
        return true;
      }
    }
    return false;
  }

  _collectNearbyPlayers() {
    const bot = this.bot;
    if (!this.connected || !bot || !bot.players || typeof bot.players !== "object") {
      return [];
    }
    const selfName = typeof bot.username === "string" ? bot.username.toLowerCase() : "";
    const names = new Set();
    for (const p of Object.values(bot.players)) {
      if (!p || typeof p.username !== "string" || !p.username) continue;
      if (selfName && p.username.toLowerCase() === selfName) continue;
      const cleanName = sanitizeMcText(p.username, 24);
      if (cleanName) {
        names.add(cleanName);
      }
    }
    return Array.from(names).sort().slice(0, 6);
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

  _computeTimeOfDay(bot) {
    if (!bot || !this.connected) return "";
    const dim = String(this.dimension || bot.game?.dimension || "").toLowerCase();
    if (dim && !dim.includes("overworld")) return "";
    const rawTod = bot.time?.timeOfDay;
    if (typeof rawTod !== "number" || !Number.isFinite(rawTod)) return "";
    const tod = ((Math.floor(rawTod) % 24000) + 24000) % 24000;
    if (tod < 12000) return "day";
    if (tod < 13000) return "sunset";
    if (tod < 23000) return "night";
    return "sunrise";
  }

  _collectNearbyResources(bot, maxDistance = 16) {
    if (!bot || !this.connected || typeof bot.findBlocks !== "function" || !bot.entity?.position) {
      return [];
    }
    try {
      const positions =
        bot.findBlocks({
          matching: (b) => Boolean(b && (NEARBY_RESOURCE_BLOCKS.has(b.name) || isBedName(b.name))),
          point: bot.entity.position,
          maxDistance,
          count: 48,
        }) || [];
      const seen = new Set();
      const ordered = [];
      for (const p of positions) {
        if (ordered.length >= 12) break;
        const blk = typeof bot.blockAt === "function" ? bot.blockAt(p) : null;
        const name = blk?.name;
        if (!name || (!NEARBY_RESOURCE_BLOCKS.has(name) && !isBedName(name)) || seen.has(name)) {
          continue;
        }
        seen.add(name);
        ordered.push(name);
      }
      return ordered;
    } catch {
      return [];
    }
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
      if (
        this.connected &&
        this.soloMaxSessionMs > 0 &&
        !this._hasOtherPlayer(this.bot) &&
        this._soloSinceMs !== null &&
        this.nowFn() - this._soloSinceMs >= this.soloMaxSessionMs
      ) {
        this._cancelActiveGoal();
        this._desiredConnected = false;
        this._manualDisconnect = true;
        this._clearReconnectTimer();
        this._teardownBot(true);
        this.activity = "待机";
        this._emitMilestone("mc_idle_disconnected", "normal", { reason: "solo_session_limit" });
        this._emitSnapshot();
        return;
      }
      if (
        this.connected &&
        !this._activeGoalCancel &&
        this.idleDisconnectMs > 0 &&
        !this._hasOtherPlayer(this.bot) &&
        this.nowFn() - this._lastActivityOrHumanAtMs >= this.idleDisconnectMs
      ) {
        this._desiredConnected = false;
        this._manualDisconnect = true;
        this._clearReconnectTimer();
        this._teardownBot(true);
        this.activity = "待机";
        this._emitMilestone("mc_idle_disconnected", "normal", { reason: "idle_alone" });
        this._emitSnapshot();
        return;
      }
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

  _startSurvivalLoop() {
    this._stopSurvivalLoop();
    if (!this.connected) return;
    this._survivalTimer = setInterval(() => {
      if (!this.connected || this._stopped) {
        this._stopSurvivalLoop();
        return;
      }
      void this._survivalTick();
    }, this.survivalTickMs);
    if (typeof this._survivalTimer.unref === "function") {
      this._survivalTimer.unref();
    }
  }

  _stopSurvivalLoop() {
    if (this._survivalTimer) {
      clearInterval(this._survivalTimer);
      this._survivalTimer = null;
    }
  }

  async _survivalTick() {
    const bot = this.bot;
    if (!this.connected || !bot || this.paused || this._stopped || this._survivalTickRunning) {
      return;
    }
    this._survivalTickRunning = true;
    try {
      const nextHealth = readNonNegativeFinite(bot.health);
      if (nextHealth !== null) this.health = nextHealth;
      const nextFood = readNonNegativeFinite(bot.food);
      if (nextFood !== null) this.food = nextFood;

      if (this.config.mcAutoEat !== false) {
        if (!this._eatingLock.eating && shouldTriggerAutoEat(this.health, this.food)) {
          await performAutoEat(bot, this._eatingLock, {
            health: this.health,
            eatTimeoutMs: this.eatTimeoutMs,
            onBeforeEat: () => lowerShield(bot, this._combatState),
          });
          this._refreshFromBot();
        }
        if (bot.inventory) {
          const safeFoodCount = countSafeFood(bot);
          if (safeFoodCount <= LOW_FOOD_THRESHOLD) {
            const nowMs = this.nowFn();
            if (nowMs - this._lastLowFoodAtMs >= this.lowFoodCooldownMs) {
              const imp =
                safeFoodCount === 0 && this.food !== null && this.food <= 10 ? "high" : "normal";
              const emitted = this._emitMilestone("mc_low_food", imp, {
                food: this.food,
                health: this.health,
                safe_food_count: safeFoodCount,
                dimension: this.dimension || "overworld",
              });
              if (emitted) {
                this._lastLowFoodAtMs = nowMs;
              }
            }
          }
        }
      }

      if (
        this.config.mcAutoEquip !== false &&
        !this._eatingLock.eating &&
        !this._activeGoalCancel &&
        bot.inventory
      ) {
        const equipped = await autoEquipBestGear(bot, { includeShield: true });
        if (equipped.length > 0) {
          this._refreshFromBot();
          this._emitSnapshot();
        }
      }
    } catch {
      // Never crash survival reflex tick
    } finally {
      this._survivalTickRunning = false;
    }
  }

  _triggerUrgentEat() {
    const bot = this.bot;
    if (
      this.config.mcAutoEat === false ||
      this._eatingLock.eating ||
      !this.connected ||
      !bot ||
      this.paused ||
      this._stopped
    ) {
      return;
    }
    void performAutoEat(bot, this._eatingLock, {
      health: this.health,
      eatTimeoutMs: this.eatTimeoutMs,
      onBeforeEat: () => lowerShield(bot, this._combatState),
    })
      .then(() => {
        if (this.connected && this.bot === bot) {
          this._refreshFromBot();
        }
      })
      .catch(() => {});
  }

  _clearCombatMicroTimer() {
    if (this._combatMicroTimer) {
      clearTimeout(this._combatMicroTimer);
      this._combatMicroTimer = null;
    }
  }

  _scheduleCombatMicroTick(cooldownMs = 625) {
    if (this._combatMicroTimer || !this.connected || this._stopped || this.paused) return;
    const delayMs = Math.max(80, Math.min(350, Math.floor((cooldownMs || 625) / 2)));
    this._combatMicroTimer = setTimeout(() => {
      this._combatMicroTimer = null;
      if (this.connected && !this._stopped && !this.paused && this.combatMode === "guard") {
        void this._companionTick();
      }
    }, delayMs);
    if (typeof this._combatMicroTimer.unref === "function") {
      this._combatMicroTimer.unref();
    }
  }

  _startCompanionLoop() {
    this._stopCompanionLoop();
    if (!this.connected) return;
    if (!this.config.mcCompanionMode && this.combatMode !== "guard") return;
    this._lastSoloWanderAtMs = this.nowFn() - Math.floor(this.soloWanderIntervalMs * 0.6);
    this._companionTimer = setInterval(() => {
      if (!this.connected || this._stopped) {
        this._stopCompanionLoop();
        return;
      }
      void this._companionTick();
    }, this.companionTickMs);
    if (typeof this._companionTimer.unref === "function") {
      this._companionTimer.unref();
    }
  }

  _stopCompanionLoop() {
    this._clearCombatMicroTimer();
    if (this._companionTimer) {
      clearInterval(this._companionTimer);
      this._companionTimer = null;
    }
  }

  _distanceBetween(a, b) {
    if (!a || !b) return Infinity;
    if (typeof a.distanceTo === "function") {
      const d = a.distanceTo(b);
      return Number.isFinite(d) ? d : Infinity;
    }
    return Math.hypot((b.x ?? 0) - (a.x ?? 0), (b.y ?? 0) - (a.y ?? 0), (b.z ?? 0) - (a.z ?? 0));
  }

  _lookAtEntity(bot, entity) {
    if (!bot || typeof bot.lookAt !== "function" || !entity?.position) return;
    const pos = entity.position;
    const eyeTarget =
      typeof pos.offset === "function"
        ? pos.offset(0, 1.62, 0)
        : { x: pos.x ?? 0, y: (pos.y ?? 0) + 1.62, z: pos.z ?? 0 };
    try {
      const res = bot.lookAt(eyeTarget, true);
      if (res && typeof res.catch === "function") {
        res.catch(() => {});
      }
    } catch {
      // Ignore lookAt errors
    }
  }

  _tryCompanionChat(rawMsg) {
    const bot = this.bot;
    if (!this.config.mcAllowChat || !this.connected || !bot || typeof bot.chat !== "function") {
      return false;
    }
    const clean = sanitizeMcText(rawMsg, 100);
    if (!clean || clean.startsWith("/")) return false;
    const nowMs = this.nowFn();
    if (this.chatCooldownMs > 0 && nowMs - this._lastChatAtMs < this.chatCooldownMs) {
      return false;
    }
    if (
      this.companionReplyCooldownMs > 0 &&
      nowMs - this._lastAutoReplyAtMs < this.companionReplyCooldownMs
    ) {
      return false;
    }
    try {
      bot.chat(clean);
      this._lastChatAtMs = nowMs;
      this._lastAutoReplyAtMs = nowMs;
      return true;
    } catch {
      return false;
    }
  }

  _findNearestAllowedPlayer(bot) {
    if (!bot?.entity?.position || this.config.mcAllowedPlayers.size === 0) return null;
    const origin = bot.entity.position;
    let best = null;
    for (const p of Object.values(bot.players || {})) {
      if (!p || !p.username || p.username === bot.username || !p.entity?.position) continue;
      if (!this.config.mcAllowedPlayers.has(p.username.toLowerCase())) continue;
      const dist = this._distanceBetween(origin, p.entity.position);
      if (!best || dist < best.dist) {
        best = { player: p, dist };
      }
    }
    return best;
  }

  _findNearestHostile(bot, maxDist = MELEE_EVADE_RADIUS) {
    if (!bot?.entity?.position) return null;
    const origin = bot.entity.position;
    let best = null;
    for (const ent of Object.values(bot.entities || {})) {
      if (!ent || ent === bot.entity || !ent.position) continue;
      const name = String(ent.name || "").toLowerCase();
      if (!HOSTILE_MOBS.has(name)) continue;
      const dist = this._distanceBetween(origin, ent.position);
      if (dist <= maxDist && (!best || dist < best.dist)) {
        best = { entity: ent, name, dist };
      }
    }
    return best;
  }

  _findEvadeThreats(bot) {
    if (!bot?.entity?.position) return null;
    const origin = bot.entity.position;
    const threats = [];
    let nearest = null;
    let hasRanged = false;

    for (const ent of Object.values(bot.entities || {})) {
      if (!ent || ent === bot.entity || !ent.position) continue;
      const name = String(ent.name || "").toLowerCase();
      if (!HOSTILE_MOBS.has(name)) continue;
      const isRanged = RANGED_HOSTILE_MOBS.has(name);
      const radius = isRanged ? RANGED_EVADE_RADIUS : MELEE_EVADE_RADIUS;
      const dist = this._distanceBetween(origin, ent.position);
      if (dist <= radius) {
        const entry = { entity: ent, name, dist, isRanged };
        threats.push(entry);
        if (isRanged) hasRanged = true;
        if (!nearest || dist < nearest.dist) {
          nearest = entry;
        }
      }
    }
    if (threats.length === 0) return null;

    let sumX = 0;
    let sumZ = 0;
    for (const t of threats) {
      sumX += t.entity.position.x ?? 0;
      sumZ += t.entity.position.z ?? 0;
    }
    return {
      threats,
      nearest,
      hasRanged,
      centroid: { x: sumX / threats.length, z: sumZ / threats.length },
    };
  }

  _computeFleeTarget(origin, threatInfo, allowed = null) {
    const fleeDist = threatInfo.hasRanged ? RANGED_FLEE_DISTANCE : MELEE_FLEE_DISTANCE;
    const dx = (origin.x ?? 0) - (threatInfo.centroid.x ?? 0);
    const dz = (origin.z ?? 0) - (threatInfo.centroid.z ?? 0);
    const len = Math.hypot(dx, dz);
    const dirX = len > 0.05 ? dx / len : 1;
    const dirZ = len > 0.05 ? dz / len : 0;
    let tx = Math.round((origin.x ?? 0) + dirX * fleeDist);
    let tz = Math.round((origin.z ?? 0) + dirZ * fleeDist);
    if (
      allowed &&
      allowed.dist <= 24 &&
      this._distanceBetween(allowed.player.entity.position, threatInfo.nearest.entity.position) >
        threatInfo.nearest.dist + 2
    ) {
      tx = Math.round(allowed.player.entity.position.x ?? tx);
      tz = Math.round(allowed.player.entity.position.z ?? tz);
    }
    return { tx, tz, fleeDist };
  }

  _findGuardedPlayer(bot) {
    if (!bot?.entity?.position || this.config.mcAllowedPlayers.size === 0) return null;
    if (this.guardTarget) {
      const targetLower = this.guardTarget.toLowerCase();
      if (this.config.mcAllowedPlayers.has(targetLower)) {
        for (const p of Object.values(bot.players || {})) {
          if (
            p &&
            typeof p.username === "string" &&
            p.username.toLowerCase() === targetLower &&
            p.username !== bot.username &&
            p.entity?.position
          ) {
            return { player: p, dist: this._distanceBetween(bot.entity.position, p.entity.position) };
          }
        }
      }
    }
    return this._findNearestAllowedPlayer(bot);
  }

  _handleCreeperAlert(creeper, nearbyPlayer) {
    const nowMs = this.nowFn();
    this._emitMilestone("mc_creeper_alert", "high", {
      distance: roundCoord(creeper.dist),
      pos: this._vecToTuple(creeper.entity?.position),
      dimension: this.dimension || "overworld",
      combat_mode: this.combatMode,
      action: "retreat_standoff",
    });
    if (nearbyPlayer && nowMs - this._lastHostileAlertAtMs >= this.hostileAlertCooldownMs) {
      if (this._tryCompanionChat("小心！旁边有苦力怕，我先拉远距离躲开喵~")) {
        this._lastHostileAlertAtMs = nowMs;
      }
    }
  }

  _handleCombatRetreatAlert({ health, food }) {
    this._emitMilestone("mc_combat_retreat", "high", {
      health,
      food,
      reason: "low_health",
      resume_health: 12,
      dimension: this.dimension || "overworld",
    });
  }

  async _companionTick() {
    const bot = this.bot;
    if (
      (!this.config.mcCompanionMode && this.combatMode !== "guard") ||
      !this.connected ||
      !bot ||
      this.paused ||
      this._stopped ||
      this._activeGoalCancel ||
      this._companionTickRunning
    ) {
      return;
    }
    const origin = bot.entity?.position;
    if (!origin) return;

    this._companionTickRunning = true;
    try {
      const nowMs = this.nowFn();
      const nextHealth = readNonNegativeFinite(bot.health);
      if (nextHealth !== null) this.health = nextHealth;
      const nextFood = readNonNegativeFinite(bot.food);
      if (nextFood !== null) this.food = nextFood;

      const allowed =
        this.combatMode === "guard"
          ? this._findGuardedPlayer(bot)
          : this._findNearestAllowedPlayer(bot);

      // 1. Combat / Self-preservation layer
      if (this.combatMode === "guard") {
        const combatRes = await runGuardCombatStep({
          bot,
          combatState: this._combatState,
          guardedPlayer: allowed?.player ?? null,
          health: this.health,
          food: this.food,
          nowMs,
          autoEquip: this.config.mcAutoEquip !== false,
          eating: this._eatingLock.eating,
          lookAtEntityFn: (b, e) => this._lookAtEntity(b, e),
          onNeedHealEat: () => this._triggerUrgentEat(),
          onCreeperAlert: (creeper) => this._handleCreeperAlert(creeper, allowed?.player ?? null),
          onRetreatAlert: (info) => this._handleCombatRetreatAlert(info),
        });
        if (combatRes.handled) {
          this._followingPlayer = null;
          this.activity = sanitizeMcText(combatRes.activity, 64) || "护卫警戒";
          if (combatRes.inCombat) {
            this._scheduleCombatMicroTick(combatRes.cooldownMs);
          } else {
            this._clearCombatMicroTimer();
          }
          return;
        }
        this._clearCombatMicroTimer();
      } else {
        this._clearCombatMicroTimer();
        lowerShield(bot, this._combatState);
        const creeper = findNearestCreeper(bot, CREEPER_SAFE_DISTANCE);
        if (creeper) {
          const dest = computeRetreatTarget(
            origin,
            creeper.entity.position,
            10,
            allowed?.player?.entity?.position ?? null,
          );
          applyNoDigMovements(bot);
          try {
            bot.pathfinder?.setGoal?.(new GoalNearXZ(dest.x, dest.z, 2), true);
          } catch {
            // Ignore pathfinder errors
          }
          this._followingPlayer = null;
          this.activity = "远离苦力怕";
          this._handleCreeperAlert(creeper, allowed?.player ?? null);
          return;
        }
        const threatInfo = this._findEvadeThreats(bot);
        if (threatInfo) {
          const { tx, tz } = this._computeFleeTarget(origin, threatInfo, allowed);
          applyNoDigMovements(bot);
          try {
            bot.setSprinting?.(true);
            bot.pathfinder?.setGoal?.(new GoalNearXZ(tx, tz, 2), true);
          } catch {
            // Ignore pathfinder errors
          }
          this._followingPlayer = null;
          this.activity = "躲避怪物";
          if (allowed && nowMs - this._lastHostileAlertAtMs >= this.hostileAlertCooldownMs) {
            if (this._tryCompanionChat("呀！旁边有怪物靠近我，快救救猫猫喵~")) {
              this._lastHostileAlertAtMs = nowMs;
            }
          }
          return;
        }
      }

      // 2. Accompany nearest / guarded whitelisted player (look + continuous follow)
      if (allowed) {
        const uname = sanitizeMcText(allowed.player.username, 20);
        const key = allowed.player.username.toLowerCase();
        this._lookAtEntity(bot, allowed.player.entity);
        if (!this._greetedPlayers.has(key)) {
          this._greetedPlayers.add(key);
          try {
            bot.swingArm?.("right");
          } catch {
            // Ignore swing errors
          }
          this._tryCompanionChat("来啦！看到你上线了喵~");
        }
        if (!this._stayPut && allowed.dist > 4.2) {
          if (this.combatMode === "guard") {
            applyNoDigMovements(bot);
          } else {
            this._ensurePathfinder(bot);
          }
          try {
            bot.pathfinder?.setGoal?.(new GoalFollow(allowed.player.entity, 3), true);
            this._followingPlayer = allowed.player.username;
            this.activity = this.combatMode === "guard" ? `护卫:${uname}` : `陪伴:${uname}`;
          } catch {
            // Ignore pathfinder errors
          }
        } else if (allowed.dist <= 3.6 && this._followingPlayer) {
          try {
            bot.pathfinder?.setGoal?.(null);
          } catch {
            // Ignore stop errors
          }
          this._followingPlayer = null;
          this.activity = this._stayPut
            ? `原地等${uname}`
            : this.combatMode === "guard"
              ? `守护${uname}`
              : `陪在${uname}身边`;
        }
        return;
      }

      // 3. Gentle solo stroll when alone (only when MC_COMPANION_MODE is enabled)
      if (
        this.config.mcCompanionMode &&
        !this._stayPut &&
        nowMs - this._lastSoloWanderAtMs >= this.soloWanderIntervalMs
      ) {
        this._lastSoloWanderAtMs = nowMs;
        const dx = (Math.random() < 0.5 ? -1 : 1) * (6 + Math.floor(Math.random() * 6));
        const dz = (Math.random() < 0.5 ? -1 : 1) * (6 + Math.floor(Math.random() * 6));
        const wx = Math.round((origin.x ?? 0) + dx);
        const wz = Math.round((origin.z ?? 0) + dz);
        this._ensurePathfinder(bot);
        try {
          bot.pathfinder?.setGoal?.(new GoalNearXZ(wx, wz, 2), false);
          this.activity = "四周闲逛";
        } catch {
          // Ignore wander errors
        }
      }
    } catch {
      // Never crash companion/combat loop
    } finally {
      this._companionTickRunning = false;
    }
  }

  _handleCompanionChat(username, rawMessage) {
    if (
      (!this.config.mcCompanionMode && this.combatMode !== "guard") ||
      this.paused ||
      !this.connected ||
      !this.bot
    ) {
      return;
    }
    if (!username || !this.config.mcAllowedPlayers.has(username.toLowerCase())) return;
    const trimmed = String(rawMessage || "").trim();
    if (!trimmed || trimmed.startsWith("/")) return;
    const cleanUser = sanitizeMcText(username, 20);
    const cleanMsg = sanitizeMcText(trimmed, 80);
    if (!cleanMsg) return;
    this._lastActivityOrHumanAtMs = this.nowFn();
    this._emitMilestone("mc_player_chat", "normal", {
      player: cleanUser,
      message: cleanMsg,
    });
    const matched = Object.values(this.bot.players || {}).find(
      (p) =>
        p &&
        typeof p.username === "string" &&
        p.username.toLowerCase() === username.toLowerCase(),
    );
    if (matched?.entity) {
      this._lookAtEntity(this.bot, matched.entity);
    }
    if (this.config.mcCompanionLocalChatReply === false) {
      return;
    }
    if (EVADE_CMD_RE.test(cleanMsg)) {
      this.combatMode = "evade";
      this.guardTarget = "";
      this._clearCombatMicroTimer();
      lowerShield(this.bot, this._combatState);
      this._combatState.retreating = false;
      this._combatState.activeTargetId = null;
      this.activity = "躲避模式";
      this._tryCompanionChat("好哒，切回躲避模式，遇到怪我会先躲开喵~");
      this._emitSnapshot();
      return;
    }
    if (GUARD_CMD_RE.test(cleanMsg)) {
      this.combatMode = "guard";
      this.guardTarget = normalizePlayerId(matched?.username || cleanUser);
      this._stayPut = false;
      this.activity = this.guardTarget ? `护卫:${this.guardTarget}` : "护卫警戒";
      if (!this._companionTimer) {
        this._startCompanionLoop();
      }
      this._tryCompanionChat("收到！进入护卫模式，猫猫来保护你喵~");
      this._emitSnapshot();
      return;
    }
    if (STAY_CMD_RE.test(cleanMsg)) {
      this._stayPut = true;
      this._followingPlayer = null;
      try {
        this.bot.pathfinder?.stop?.();
        this.bot.pathfinder?.setGoal?.(null);
      } catch {
        // Ignore stop errors
      }
      this.activity = `原地等${cleanUser}`;
      this._tryCompanionChat("好哒，我乖乖站在这里等你喵~");
      this._emitSnapshot();
      return;
    }
    if (FOLLOW_CMD_RE.test(cleanMsg)) {
      this._stayPut = false;
      if (matched?.entity) {
        this._ensurePathfinder(this.bot);
        try {
          this.bot.pathfinder?.setGoal?.(new GoalFollow(matched.entity, 3), true);
          this._followingPlayer = matched.username;
        } catch {
          // Ignore follow errors
        }
      }
      this.activity = `陪伴:${cleanUser}`;
      this._tryCompanionChat("来啦来啦！这就跟上你喵~");
      this._emitSnapshot();
      return;
    }
    if (GREET_CMD_RE.test(cleanMsg)) {
      try {
        this.bot.swingArm?.("right");
      } catch {
        // Ignore swing errors
      }
      const hostile = this._findNearestHostile(this.bot, 12);
      const reply =
        hostile || (this.health !== null && this.health <= 16)
          ? "在呢在呢！刚才旁边有怪欺负我，快保护猫猫喵~"
          : "在呢！我看到你啦，这就陪着你一起玩喵~";
      this._tryCompanionChat(reply);
      this._emitSnapshot();
    }
  }

  _clearReconnectTimer() {
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
  }

  _powerdHeaders(hasJsonBody = false) {
    const headers = {};
    if (this.config.mcPowerdToken) {
      headers.Authorization = `Bearer ${this.config.mcPowerdToken}`;
    }
    if (hasJsonBody) {
      headers["Content-Type"] = "application/json";
    }
    return headers;
  }

  _cancelWakePoll() {
    if (this._wakeController) {
      const ctrl = this._wakeController;
      this._wakeController = null;
      if (!ctrl.signal.aborted) {
        ctrl.abort();
      }
    }
  }

  _sleepAbortable(ms, signal) {
    if (signal.aborted) return Promise.resolve(false);
    return new Promise((resolve) => {
      const onAbort = () => {
        clearTimeout(timer);
        resolve(false);
      };
      const timer = setTimeout(() => {
        signal.removeEventListener("abort", onAbort);
        resolve(true);
      }, ms);
      if (typeof timer.unref === "function") {
        timer.unref();
      }
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  _startWakePoll() {
    this._cancelWakePoll();
    const controller = new AbortController();
    this._wakeController = controller;
    const signal = controller.signal;
    const wallStart = Date.now();
    const logicalStart = this.nowFn();

    (async () => {
      try {
        while (!signal.aborted && !this._stopped && this._desiredConnected && !this.paused) {
          const waited = await this._sleepAbortable(this.powerPollIntervalMs, signal);
          if (!waited || signal.aborted || this._stopped || !this._desiredConnected || this.paused) {
            return;
          }
          if (this.connected || this.bot) {
            return;
          }
          try {
            const reqSignal =
              typeof AbortSignal.any === "function"
                ? AbortSignal.any([signal, AbortSignal.timeout(4_000)])
                : AbortSignal.timeout(4_000);
            const resp = await this.fetchFn(`${this.config.mcPowerdUrl}/status`, {
              method: "GET",
              headers: this._powerdHeaders(false),
              signal: reqSignal,
            });
            if (signal.aborted || this._stopped || !this._desiredConnected || this.paused) {
              return;
            }
            if (resp && resp.ok) {
              const body = await resp.json().catch(() => ({}));
              if (body && body.ready === true) {
                if (this._wakeController === controller) {
                  this._wakeController = null;
                }
                if (!signal.aborted && !this._stopped && this._desiredConnected && !this.paused) {
                  this.connectBot();
                }
                return;
              }
              if (body && body.state === "stopped") {
                break;
              }
            }
          } catch {
            // Transient poll error; keep retrying until timeout or cancellation
          }
          const elapsed = Math.max(Date.now() - wallStart, this.nowFn() - logicalStart);
          if (elapsed >= this.powerWakeTimeoutMs) {
            break;
          }
        }
        if (
          this._wakeController === controller &&
          !signal.aborted &&
          !this._stopped &&
          !this.connected
        ) {
          this._wakeController = null;
          this._desiredConnected = false;
          if (!this.activity.startsWith("筹备建造:")) {
            this.activity = this.paused ? "已暂停" : "待机";
          }
          this._emitSnapshot();
        }
      } finally {
        if (this._wakeController === controller) {
          this._wakeController = null;
        }
      }
    })();
  }

  _scheduleReconnect() {
    this._clearReconnectTimer();
    if (
      this._stopped ||
      this._manualDisconnect ||
      !this._desiredConnected ||
      this.paused ||
      !this.config.mcEnabled ||
      !this.config.mcConfigured
    ) {
      return;
    }
    this._consecutiveReconnectFails += 1;
    if (this.config.mcAutoConnect === false && this._consecutiveReconnectFails >= 3) {
      this._desiredConnected = false;
      this._reconnectAttempts = 0;
      if (!this.activity.startsWith("筹备建造:")) {
        this.activity = "待机";
      }
      this._emitSnapshot();
      return;
    }
    const baseMs = Math.min(60_000, 5000 * 2 ** Math.min(this._reconnectAttempts, 4));
    this._reconnectAttempts += 1;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      if (!this._stopped && !this._manualDisconnect && this._desiredConnected && !this.paused) {
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

  _ensurePathfinder(bot, { allowDig = false } = {}) {
    applySafeMovements(bot, { allowDig });
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
      this._consecutiveReconnectFails = 0;
      this._lastActivityOrHumanAtMs = this.nowFn();
      this._syncSoloClock(bot);
      this.dimension = sanitizeMcText(bot.game?.dimension || "overworld", 24) || "overworld";
      this.pos = this._vecToTuple(bot.entity?.position);
      this.health = readNonNegativeFinite(bot.health);
      this.food = readNonNegativeFinite(bot.food);
      this._prevHealth = this.health;
      const invSnap = collectInventorySnapshot(bot);
      this.equipment = invSnap.equipment;
      this.inventorySummary = invSnap.inventory_summary;
      this.freeSlots = invSnap.free_slots;
      this.timeOfDay = this._computeTimeOfDay(bot);
      this.nearbyResources = this._collectNearbyResources(bot, 16);
      this.nearbyEntities = this._collectNearbyEntities(bot);
      if (
        !this.activity ||
        this.activity === "待机" ||
        this.activity === "连接中" ||
        this.activity === "唤醒MC服务端中" ||
        this.activity === "连接已断开" ||
        this.activity === "等待复活"
      ) {
        this.activity = this.paused ? "已暂停" : "原地待命";
      }
      this._startSnapshotTimer();
      this._startSurvivalLoop();
      this._startCompanionLoop();
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
      this._combatState.retreating = false;
      lowerShield(bot, this._combatState);
      const invSnap = collectInventorySnapshot(bot);
      this.equipment = invSnap.equipment;
      this.inventorySummary = invSnap.inventory_summary;
      this.freeSlots = invSnap.free_slots;
      this.timeOfDay = this._computeTimeOfDay(bot);
      this.nearbyResources = this._collectNearbyResources(bot, 16);
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
      this._clearCombatMicroTimer();
      lowerShield(bot, this._combatState);
      this._combatState.retreating = false;
      this._combatState.activeTargetId = null;
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

    bot.on("playerJoined", (player) => {
      if (this.bot !== bot || !this.connected) return;
      if (
        this._hasOtherPlayer(this.bot) ||
        (player && typeof player.username === "string" && player.username && player.username !== bot.username)
      ) {
        this._soloSinceMs = null;
      }
    });

    bot.on("playerLeft", () => {
      if (this.bot !== bot || !this.connected) return;
      this._syncSoloClock(bot);
    });

    bot.on("chat", (username, message) => {
      if (this.bot !== bot) return;
      if (username === bot.username) return;
      if (this._hasOtherPlayer(this.bot)) {
        this._soloSinceMs = null;
      }
      const cleanUser = sanitizeMcText(username, 40);
      const cleanMsg = sanitizeMcText(message, 80);
      if (!cleanUser && !cleanMsg) return;
      this.chatBuffer.push(cleanUser ? `${cleanUser}: ${cleanMsg}` : cleanMsg);
      this._handleCompanionChat(username, message);
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
    this._stopSurvivalLoop();
    this._stopCompanionLoop();
    this._stopSnapshotTimer();
    this._cancelActiveGoal();
    lowerShield(bot, this._combatState);
    this._combatState = createCombatState();
    this._eatingLock.eating = false;
    this._followingPlayer = null;
    this._greetedPlayers.clear();
    this.bot = null;
    this.connected = false;
    this._soloSinceMs = null;
    this.dimension = "";
    this.pos = null;
    this.health = null;
    this.food = null;
    this.equipment = createEmptyEquipment();
    this.inventorySummary = [];
    this.freeSlots = null;
    this.guardTarget = "";
    this.timeOfDay = "";
    this.nearbyResources = [];
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
    this._soloSinceMs = null;
    if (!bot) return;
    this.bot = null;
    this._stopSurvivalLoop();
    this._stopCompanionLoop();
    this._stopSnapshotTimer();
    this._cancelActiveGoal();
    lowerShield(bot, this._combatState);
    this._combatState = createCombatState();
    this._eatingLock.eating = false;
    this._followingPlayer = null;
    this._greetedPlayers.clear();
    const wasConnected = this.connected;
    this.connected = false;
    this.dimension = "";
    this.pos = null;
    this.health = null;
    this.food = null;
    this.equipment = createEmptyEquipment();
    this.inventorySummary = [];
    this.freeSlots = null;
    this.guardTarget = "";
    this.timeOfDay = "";
    this.nearbyResources = [];
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
    const boundedMs = Math.min(35_000, Math.max(100, timeoutMs ?? this.actionTimeoutMs));
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
      this._lastActivityOrHumanAtMs = this.nowFn();
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

    const checkMs = this.stuckCheckIntervalMs;
    const startPos = this._vecToTuple(bot.entity?.position);
    if (!checkMs || checkMs <= 0 || !startPos) {
      await bot.pathfinder.goto(goal).catch((err) => {
        if (signal?.aborted) return;
        throw err;
      });
      return;
    }

    let stuckTimer = null;
    let jumpTimer = null;
    let lastPos = startPos;
    let stuckStrikes = 0;
    let markedStuck = false;

    const cleanup = () => {
      if (stuckTimer) {
        clearInterval(stuckTimer);
        stuckTimer = null;
      }
      if (jumpTimer) {
        clearTimeout(jumpTimer);
        jumpTimer = null;
      }
      try {
        bot.setControlState?.("jump", false);
      } catch {
        // Ignore control reset errors
      }
    };

    const gotoPromise = Promise.resolve(bot.pathfinder.goto(goal)).catch((err) => {
      if (signal?.aborted || markedStuck) return;
      throw err;
    });

    const stuckPromise = new Promise((_, reject) => {
      stuckTimer = setInterval(() => {
        if (signal?.aborted) {
          cleanup();
          return;
        }
        const curPos = this._vecToTuple(bot.entity?.position);
        if (!curPos) return;
        const moved = Math.hypot(
          curPos[0] - lastPos[0],
          curPos[1] - lastPos[1],
          curPos[2] - lastPos[2],
        );
        lastPos = curPos;
        if (moved >= 0.35) {
          stuckStrikes = 0;
          return;
        }
        stuckStrikes += 1;
        if (stuckStrikes === 1) {
          try {
            bot.setControlState?.("jump", true);
            jumpTimer = setTimeout(() => {
              jumpTimer = null;
              try {
                bot.setControlState?.("jump", false);
              } catch {
                // Ignore jump release errors
              }
            }, 180);
          } catch {
            // Ignore jump trigger errors
          }
          return;
        }
        markedStuck = true;
        cleanup();
        try {
          bot.pathfinder?.stop?.();
          bot.pathfinder?.setGoal?.(null);
        } catch {
          // Ignore pathfinder stop errors
        }
        const err = new Error("path_stuck");
        err.code = "path_stuck";
        reject(err);
      }, checkMs);
    });

    try {
      await Promise.race([gotoPromise, stuckPromise]);
    } finally {
      cleanup();
    }
  }

  _findNearestDroppedItem(bot, centerCoords, maxDist = 5) {
    if (!bot || !centerCoords) return null;
    const [cx, cy, cz] = centerCoords;
    let best = null;
    for (const ent of Object.values(bot.entities || {})) {
      if (!ent || ent === bot.entity || !ent.position) continue;
      const entName = String(ent.name || "").toLowerCase();
      const entDisplay = String(ent.displayName || "").toLowerCase();
      const isDrop =
        entName === "item" ||
        entDisplay === "item" ||
        (ent.type === "object" && (entName === "item" || ent.objectType === "Item"));
      if (!isDrop) continue;
      const dist = Math.hypot(
        (ent.position.x ?? 0) - cx,
        (ent.position.y ?? 0) - cy,
        (ent.position.z ?? 0) - cz,
      );
      if (Number.isFinite(dist) && dist <= maxDist && (!best || dist < best.dist)) {
        best = { entity: ent, pos: ent.position, dist };
      }
    }
    return best;
  }

  async _collectNearbyDrops(bot, coords, signal) {
    if (!bot || !coords || signal?.aborted) return;
    if (this.pickupSettleMs > 0) {
      await this._sleepAbortable(Math.min(80, this.pickupSettleMs), signal);
      if (signal?.aborted) return;
    }

    const drop = this._findNearestDroppedItem(bot, coords, 5);
    const origin = bot.entity?.position;
    const distToBlock = origin
      ? Math.hypot(
          coords[0] - (origin.x ?? 0),
          coords[1] - (origin.y ?? 0),
          coords[2] - (origin.z ?? 0),
        )
      : 0;

    let target = null;
    if (drop?.pos) {
      target = [drop.pos.x ?? coords[0], drop.pos.y ?? coords[1], drop.pos.z ?? coords[2]];
    } else if (distToBlock > 1.5) {
      target = coords;
    }

    if (target && bot.pathfinder?.goto) {
      const subCtrl = new AbortController();
      const onParentAbort = () => subCtrl.abort();
      signal?.addEventListener?.("abort", onParentAbort, { once: true });
      let subTimer = null;
      try {
        await Promise.race([
          this._gotoGoal(new GoalNear(target[0], target[1], target[2], 1), subCtrl.signal),
          new Promise((resolve) => {
            subTimer = setTimeout(() => {
              subCtrl.abort();
              try {
                bot.pathfinder?.stop?.();
                bot.pathfinder?.setGoal?.(null);
              } catch {
                // Ignore stop errors
              }
              resolve();
            }, 2_500);
            if (typeof subTimer.unref === "function") subTimer.unref();
          }),
        ]);
      } catch {
        // Best-effort pickup; never fail a completed dig if drop fell down a hole
      } finally {
        if (subTimer) clearTimeout(subTimer);
        signal?.removeEventListener?.("abort", onParentAbort);
      }
      if (!signal?.aborted && this.pickupSettleMs > 0) {
        await this._sleepAbortable(this.pickupSettleMs, signal);
      }
    }
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
    this._cancelWakePoll();
    this._cancelActiveGoal();
    this._clearCombatMicroTimer();
    lowerShield(this.bot, this._combatState);
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

    if (
      this.paused &&
      actionType !== "status" &&
      actionType !== "stop" &&
      actionType !== "idle" &&
      actionType !== "disconnect"
    ) {
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
      this._stayPut = true;
      this._followingPlayer = null;
      this._clearCombatMicroTimer();
      lowerShield(this.bot, this._combatState);
      this._cancelActiveGoal();
      try {
        this.bot?.pathfinder?.stop?.();
        this.bot?.pathfinder?.setGoal?.(null);
      } catch {
        // Ignore stop errors
      }
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
        this._lastActivityOrHumanAtMs = this.nowFn();
        return {
          ok: true,
          accepted: true,
          bridge_ready: true,
          paused: false,
          summary: "MC已连接",
          snapshot: this.getSnapshot(),
        };
      }
      this._desiredConnected = true;
      this._manualDisconnect = false;
      this._consecutiveReconnectFails = 0;
      this._reconnectAttempts = 0;
      this._lastActivityOrHumanAtMs = this.nowFn();

      if (this.config.mcPowerdUrl) {
        try {
          const resp = await this.fetchFn(`${this.config.mcPowerdUrl}/start`, {
            method: "POST",
            headers: this._powerdHeaders(false),
            signal: AbortSignal.timeout(4_000),
          });
          const body = await resp.json().catch(() => ({}));
          if (resp.status === 503 || body?.reason === "low_memory") {
            this._desiredConnected = false;
            this._cancelWakePoll();
            return {
              ok: false,
              accepted: false,
              bridge_ready: true,
              paused: false,
              reason: "mc_host_low_memory",
              summary: "境内节点内存不足，暂不唤醒MC服务端",
              snapshot: this.getSnapshot(),
            };
          }
          if (!resp.ok || body?.ok === false) {
            this._desiredConnected = false;
            this._cancelWakePoll();
            return {
              ok: false,
              accepted: false,
              bridge_ready: true,
              paused: false,
              reason: "mc_powerd_unavailable",
              summary: "MC唤醒服务暂不可用",
              snapshot: this.getSnapshot(),
            };
          }
          if (body?.ready === true) {
            this._cancelWakePoll();
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
          this.activity = "唤醒MC服务端中";
          this._startWakePoll();
          this._emitSnapshot();
          return {
            ok: true,
            accepted: true,
            bridge_ready: true,
            paused: false,
            summary: "正在唤醒MC服务端并准备上号",
            snapshot: this.getSnapshot(),
          };
        } catch {
          this._desiredConnected = false;
          this._cancelWakePoll();
          return {
            ok: false,
            accepted: false,
            bridge_ready: true,
            paused: false,
            reason: "mc_powerd_unavailable",
            summary: "MC唤醒服务暂不可用",
            snapshot: this.getSnapshot(),
          };
        }
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
      this._desiredConnected = false;
      this._manualDisconnect = true;
      this._stayPut = false;
      this._soloSinceMs = null;
      this._cancelWakePoll();
      this._clearReconnectTimer();
      this._teardownBot(true);
      this.activity = "待机";
      this._emitSnapshot();
      if (params.sleep_server === true && this.config.mcPowerdUrl) {
        try {
          await this.fetchFn(`${this.config.mcPowerdUrl}/stop`, {
            method: "POST",
            headers: this._powerdHeaders(true),
            body: JSON.stringify({ force: false }),
            signal: AbortSignal.timeout(3_000),
          });
        } catch {
          // Best-effort server sleep request; disconnect already succeeded
        }
      }
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
    this._lastActivityOrHumanAtMs = this.nowFn();
    const timeoutMs =
      typeof params.timeout_ms === "number" && Number.isFinite(params.timeout_ms)
        ? Math.min(35_000, Math.max(100, params.timeout_ms))
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
        try {
          await this._gotoGoal(new GoalNear(coords.x, coords.y, coords.z, 1), signal);
          return { ok: true };
        } catch (err) {
          if (err?.code === "path_stuck") return { stuck: true };
          throw err;
        }
      });
      this._refreshFromBot();
      if (outcome?.stuck || outcome?.timedOut || outcome?.aborted) {
        this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
        if (this.connected) this._emitSnapshot();
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.stuck
            ? "path_stuck"
            : outcome.timedOut
              ? "mc_goal_timeout"
              : "mc_goal_cancelled",
          summary: outcome.stuck ? "寻路卡住已中止" : outcome.timedOut ? "寻路超时" : "寻路已取消",
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
        try {
          await this._gotoGoal(new GoalNearXZ(targetX, targetZ, 2), signal);
          return { ok: true };
        } catch (err) {
          if (err?.code === "path_stuck") return { stuck: true };
          throw err;
        }
      });
      this._refreshFromBot();
      const rare = this.connected ? this._scanRareBlocks(bot, 24) : [];
      const signs = this.connected ? this._scanSigns(bot, 24) : [];
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.stuck || outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.stuck
            ? "path_stuck"
            : outcome.timedOut
              ? "mc_goal_timeout"
              : "mc_goal_cancelled",
          summary: outcome.stuck ? "探索寻路卡住" : outcome.timedOut ? "探索超时" : "探索已取消",
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
      const rawCount = params.count === undefined ? 1 : Number(params.count);
      const requestedCount =
        Number.isInteger(rawCount) && rawCount >= 1 ? Math.min(8, rawCount) : 1;

      if (rawBlock) {
        const tierCheck = checkHarvestToolTier(bot, rawBlock);
        if (!tierCheck.ok) {
          this._refreshFromBot();
          this.activity = this.paused ? "已暂停" : "原地待命";
          this._emitSnapshot();
          return {
            ok: false,
            accepted: true,
            bridge_ready: true,
            paused: false,
            reason: "tool_tier_too_low",
            block: rawBlock,
            required_tool: tierCheck.requiredTool,
            best_tool: tierCheck.bestTool,
            summary: `挖掘${rawBlock}需要至少${tierCheck.requiredTool}`,
            snapshot: this.getSnapshot(),
          };
        }
      }

      const targets = new Set(
        rawBlock ? [rawBlock] : actionType === "gather" ? DEFAULT_GATHER_BLOCKS : DEFAULT_MINE_BLOCKS,
      );
      this.activity = rawBlock ? `采集:${rawBlock}` : actionType === "gather" ? "采集资源" : "采矿中";
      this._emitSnapshot();

      const findCandidatePositions = () =>
        typeof bot.findBlocks === "function"
          ? bot.findBlocks({
              matching: (b) => Boolean(b && targets.has(b.name)),
              point: bot.entity?.position,
              maxDistance: 32,
              count: Math.max(8, requestedCount * 3),
            }) || []
          : [];

      const initialPositions = findCandidatePositions();
      if (initialPositions.length === 0) {
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

      let collectedCount = 0;
      let lastBlockName = rawBlock || "block";
      let lastCoords = [0, 0, 0];
      let blockedTierInfo = null;
      const visitedKeys = new Set();

      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        for (let step = 0; step < requestedCount; step += 1) {
          if (signal.aborted) return { aborted: true };
          if (
            collectedCount > 0 &&
            this.combatMode === "evade" &&
            typeof bot.health === "number" &&
            bot.health <= 6
          ) {
            break;
          }
          const positions = step === 0 ? initialPositions : findCandidatePositions();
          const exposedCandidates = [];
          const buriedCandidates = [];
          for (const p of positions) {
            const c = this._vecToTuple(p) || [0, 0, 0];
            const key = `${Math.floor(c[0])},${Math.floor(c[1])},${Math.floor(c[2])}`;
            if (visitedKeys.has(key)) continue;
            const blk = typeof bot.blockAt === "function" ? bot.blockAt(p) : null;
            if (blk && !targets.has(blk.name)) continue;
            const candName = blk?.name || rawBlock;
            if (candName) {
              const candTier = checkHarvestToolTier(bot, candName);
              if (!candTier.ok) {
                if (!blockedTierInfo) {
                  blockedTierInfo = { block: candName, ...candTier };
                }
                continue;
              }
            }
            const entry = { blk, coords: c, key };
            if (hasExposedFace(bot, c)) {
              exposedCandidates.push(entry);
            } else {
              buriedCandidates.push(entry);
            }
          }
          const chosen = exposedCandidates[0] || buriedCandidates[0] || null;
          if (!chosen) break;
          const chosenBlock = chosen.blk;
          const chosenCoords = chosen.coords;
          visitedKeys.add(chosen.key);

          const blockName = sanitizeMcText(chosenBlock?.name || rawBlock || "block", 48);
          lastBlockName = blockName;
          lastCoords = chosenCoords;

          if (RARE_BLOCKS.has(blockName)) {
            this._emitMilestone("mc_rare_find", "high", {
              block: blockName,
              pos: chosenCoords,
              dimension: this.dimension || "overworld",
            });
          }

          if (bot.pathfinder?.goto) {
            try {
              await this._gotoGoal(
                new GoalNear(chosenCoords[0], chosenCoords[1], chosenCoords[2], 3),
                signal,
              );
            } catch (err) {
              if (err?.code === "path_stuck") {
                if (collectedCount === 0) return { stuck: true };
                break;
              }
              throw err;
            }
          }
          if (signal.aborted) return { aborted: true };
          if (chosenBlock && this.config.mcAutoEquip !== false && !this._eatingLock.eating) {
            await equipBestHarvestTool(bot, chosenBlock);
          }
          if (signal.aborted) return { aborted: true };
          if (chosenBlock && typeof bot.dig === "function") {
            await bot.dig(chosenBlock);
          }
          collectedCount += 1;
          await this._collectNearbyDrops(bot, chosenCoords, signal);
        }
        return { ok: collectedCount > 0 };
      });

      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (collectedCount === 0 && blockedTierInfo && !outcome?.stuck && !outcome?.timedOut && !outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: "tool_tier_too_low",
          block: blockedTierInfo.block,
          required_tool: blockedTierInfo.requiredTool,
          best_tool: blockedTierInfo.bestTool,
          summary: `挖掘${blockedTierInfo.block}需要至少${blockedTierInfo.requiredTool}`,
          snapshot: this.getSnapshot(),
        };
      }
      if ((outcome?.stuck || outcome?.timedOut || outcome?.aborted) && collectedCount === 0) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.stuck
            ? "path_stuck"
            : outcome.timedOut
              ? "mc_goal_timeout"
              : "mc_goal_cancelled",
          summary: outcome.stuck ? "采集寻路卡住" : outcome.timedOut ? "采集超时" : "采集已取消",
          snapshot: this.getSnapshot(),
        };
      }
      const summary =
        collectedCount > 1
          ? `已采集${lastBlockName} x${collectedCount}(${lastCoords.join(",")})`
          : `已采集${lastBlockName}(${lastCoords.join(",")})`;
      this._emitMilestone("mc_goal_completed", "normal", {
        action_type: actionType,
        summary,
        block: lastBlockName,
        collected_count: collectedCount,
      });
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        summary,
        block: lastBlockName,
        pos: lastCoords,
        collected_count: collectedCount,
        requested_count: requestedCount,
        partial: collectedCount < requestedCount,
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
      this._stayPut = false;
      this._followingPlayer = matched.username;
      this.activity = `跟随:${sanitizeMcText(matched.username, 24)}`;
      this._emitSnapshot();
      const followTimeout = Math.min(15_000, timeoutMs);
      const outcome = await this._runBoundedGoal(actionType, followTimeout, async (signal) => {
        await this._gotoGoal(new GoalFollow(matched.entity, 2), signal);
        return { ok: true };
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
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

    if (actionType === "craft") {
      const parsed = parseCraftParams(params);
      if (parsed.error) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: parsed.error,
          summary: parsed.error === "invalid_item_name" ? "合成物品ID无效" : "合成数量需为1到16的整数",
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = `合成:${parsed.itemName}`;
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        return await executeCraftAction(bot, params, {
          gotoFn: (goal, sig) => this._gotoGoal(goal, sig),
          autoEquip: this.config.mcAutoEquip !== false,
          signal,
        });
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "合成超时" : "合成已取消",
          item_name: parsed.itemName,
          requested_count: parsed.count,
          crafted_count: 0,
          missing_materials: [],
          snapshot: this.getSnapshot(),
        };
      }
      if (outcome?.ok) {
        this._emitMilestone("mc_goal_completed", "normal", {
          action_type: "craft",
          summary: outcome.summary,
          item_name: outcome.item_name,
          crafted_count: outcome.crafted_count,
        });
        return {
          ok: true,
          accepted: true,
          bridge_ready: true,
          paused: false,
          ...outcome,
          snapshot: this.getSnapshot(),
        };
      }
      return {
        ok: false,
        accepted: outcome?.accepted ?? true,
        bridge_ready: true,
        paused: false,
        reason: outcome?.reason || "craft_failed",
        summary: outcome?.summary || `合成${parsed.itemName}失败`,
        item_name: outcome?.item_name ?? parsed.itemName,
        requested_count: outcome?.requested_count ?? parsed.count,
        crafted_count: outcome?.crafted_count ?? 0,
        missing_materials: Array.isArray(outcome?.missing_materials)
          ? outcome.missing_materials
          : [],
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "smelt") {
      const parsed = parseSmeltParams(params);
      if (parsed.error) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: parsed.error,
          summary:
            parsed.error === "invalid_item_name"
              ? "熔炼物品ID无效"
              : parsed.error === "invalid_fuel_name"
                ? "燃料物品ID无效"
                : "熔炼数量需为1到16的整数",
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = `熔炼:${parsed.itemName}`;
      this._emitSnapshot();
      const smeltTimeoutMs =
        typeof params.timeout_ms === "number" && Number.isFinite(params.timeout_ms)
          ? Math.min(35_000, Math.max(100, params.timeout_ms))
          : Math.min(32_000, Math.max(timeoutMs, 28_000));
      const maxSmeltWaitMs = Math.max(200, smeltTimeoutMs - 3_000);
      const outcome = await this._runBoundedGoal(actionType, smeltTimeoutMs, async (signal) => {
        return await executeSmeltAction(bot, params, {
          gotoFn: (goal, sig) => this._gotoGoal(goal, sig),
          signal,
          pollIntervalMs: this.smeltPollIntervalMs,
          maxSmeltWaitMs,
        });
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "熔炼超时" : "熔炼已取消",
          item_name: parsed.itemName,
          output_item: "",
          requested_count: parsed.count,
          smelted_count: 0,
          missing_materials: [],
          snapshot: this.getSnapshot(),
        };
      }
      if (outcome?.ok) {
        this._emitMilestone("mc_goal_completed", "normal", {
          action_type: "smelt",
          summary: outcome.summary,
          item_name: outcome.item_name,
          output_item: outcome.output_item,
          smelted_count: outcome.smelted_count,
        });
        return {
          ok: true,
          accepted: true,
          bridge_ready: true,
          paused: false,
          ...outcome,
          snapshot: this.getSnapshot(),
        };
      }
      return {
        ok: false,
        accepted: outcome?.accepted ?? true,
        bridge_ready: true,
        paused: false,
        reason: outcome?.reason || "smelt_failed",
        summary: outcome?.summary || `熔炼${parsed.itemName}失败`,
        item_name: outcome?.item_name ?? parsed.itemName,
        output_item: outcome?.output_item ?? "",
        requested_count: outcome?.requested_count ?? parsed.count,
        smelted_count: outcome?.smelted_count ?? 0,
        missing_materials: Array.isArray(outcome?.missing_materials)
          ? outcome.missing_materials
          : [],
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "guard") {
      const rawTarget =
        params.target_player !== undefined
          ? params.target_player
          : params.player !== undefined
            ? params.player
            : "";
      const trimmedTarget = typeof rawTarget === "string" ? rawTarget.trim() : "";
      let resolvedTarget = "";
      if (trimmedTarget) {
        const cleanTarget = normalizePlayerId(trimmedTarget);
        if (
          !cleanTarget ||
          this.config.mcAllowedPlayers.size === 0 ||
          !this.config.mcAllowedPlayers.has(cleanTarget.toLowerCase())
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
        const matchedOnline = Object.values(bot.players || {}).find(
          (p) =>
            p &&
            typeof p.username === "string" &&
            p.username.toLowerCase() === cleanTarget.toLowerCase(),
        );
        const matchedAllowed = this.config.mcAllowedPlayerList.find(
          (u) => u.toLowerCase() === cleanTarget.toLowerCase(),
        );
        resolvedTarget = normalizePlayerId(matchedOnline?.username || matchedAllowed || cleanTarget);
      }

      this.combatMode = "guard";
      this.guardTarget = resolvedTarget;
      this._stayPut = false;
      if (!this._companionTimer) {
        this._startCompanionLoop();
      }
      this._refreshFromBot();
      const effectiveTarget = this._resolveGuardTargetName();
      this.activity = effectiveTarget ? `护卫:${effectiveTarget}` : "护卫警戒";
      this._emitSnapshot();
      const summary = effectiveTarget
        ? `已切换护卫模式(守护${effectiveTarget})`
        : "已切换护卫模式";
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        combat_mode: "guard",
        guard_target: effectiveTarget,
        summary,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "evade") {
      this.combatMode = "evade";
      this.guardTarget = "";
      this._clearCombatMicroTimer();
      lowerShield(bot, this._combatState);
      this._combatState.retreating = false;
      this._combatState.activeTargetId = null;
      if (!this.config.mcCompanionMode) {
        this._stopCompanionLoop();
      }
      let fledTo = null;
      const origin = bot.entity?.position;
      const threatInfo = origin ? this._findEvadeThreats(bot) : null;
      if (origin && threatInfo) {
        const allowed = this._findNearestAllowedPlayer(bot);
        const { tx, tz } = this._computeFleeTarget(origin, threatInfo, allowed);
        applyNoDigMovements(bot);
        try {
          bot.setSprinting?.(true);
          bot.pathfinder?.setGoal?.(new GoalNearXZ(tx, tz, 2), true);
        } catch {
          // Ignore pathfinder errors
        }
        fledTo = [tx, tz];
      }
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : fledTo ? "远离威胁" : "原地待命";
      this._emitSnapshot();
      return {
        ok: true,
        accepted: true,
        bridge_ready: true,
        paused: false,
        combat_mode: "evade",
        guard_target: "",
        ...(fledTo ? { fled_to: fledTo } : {}),
        summary: fledTo ? `已切换躲避模式并向(${fledTo.join(",")})撤离` : "已切换躲避模式",
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "hunt") {
      const parsed = parseHuntParams(params);
      if (parsed.error) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: parsed.error,
          summary: `狩猎参数无效:${parsed.error}`,
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = parsed.targetMob ? `狩猎:${parsed.targetMob}` : "狩猎觅食";
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        return await executeHuntAction(bot, params, {
          signal,
          gotoGoal: (goal, sig) => this._gotoGoal(goal, sig),
          collectDrops: (b, c, sig) => this._collectNearbyDrops(b, c, sig),
          attackCooldownOverrideMs: this.huntAttackCooldownMs,
        });
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "狩猎超时" : "狩猎已取消",
          snapshot: this.getSnapshot(),
        };
      }
      if (outcome?.ok) {
        this._emitMilestone("mc_goal_completed", "normal", {
          action_type: "hunt",
          summary: outcome.summary,
          target_mob: outcome.target_mob,
          hunted_count: outcome.hunted_count,
        });
      }
      return {
        ok: Boolean(outcome?.ok),
        accepted: true,
        bridge_ready: true,
        paused: false,
        ...outcome,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "sleep") {
      this.activity = "找床睡觉";
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        return await executeSleepAction(bot, params, {
          signal,
          gotoGoal: (goal, sig) => this._gotoGoal(goal, sig),
        });
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "睡觉超时" : "睡觉已取消",
          snapshot: this.getSnapshot(),
        };
      }
      if (outcome?.ok) {
        this._emitMilestone("mc_goal_completed", "normal", {
          action_type: "sleep",
          summary: outcome.summary,
          bed_pos: outcome.bed_pos,
        });
      }
      return {
        ok: Boolean(outcome?.ok),
        accepted: true,
        bridge_ready: true,
        paused: false,
        ...outcome,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "chest") {
      const parsed = parseChestParams(params);
      if (parsed.error) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: parsed.error,
          summary: `箱子参数无效:${parsed.error}`,
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = parsed.mode === "withdraw" ? "箱子取物" : parsed.mode === "inspect" ? "查看箱子" : "箱子存物";
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        return await executeChestAction(bot, params, {
          signal,
          gotoGoal: (goal, sig) => this._gotoGoal(goal, sig),
        });
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "箱子交互超时" : "箱子交互已取消",
          snapshot: this.getSnapshot(),
        };
      }
      if (outcome?.ok) {
        this._emitMilestone("mc_goal_completed", "normal", {
          action_type: "chest",
          mode: outcome.mode,
          summary: outcome.summary,
          chest_pos: outcome.chest_pos,
        });
      }
      return {
        ok: Boolean(outcome?.ok),
        accepted: true,
        bridge_ready: true,
        paused: false,
        ...outcome,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "place") {
      const parsed = parsePlaceParams(params);
      if (parsed.error) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: parsed.error,
          summary: `放置参数无效:${parsed.error}`,
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = `放置:${parsed.itemName}`;
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        return await executePlaceAction(bot, params, { signal });
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "放置超时" : "放置已取消",
          snapshot: this.getSnapshot(),
        };
      }
      if (outcome?.ok) {
        this._emitMilestone("mc_goal_completed", "normal", {
          action_type: "place",
          item_name: outcome.item_name,
          placed_pos: outcome.placed_pos,
          summary: outcome.summary,
        });
      }
      return {
        ok: Boolean(outcome?.ok),
        accepted: true,
        bridge_ready: true,
        paused: false,
        ...outcome,
        snapshot: this.getSnapshot(),
      };
    }

    if (actionType === "give") {
      const parsed = parseGiveParams(params);
      if (parsed.error) {
        return {
          ok: false,
          accepted: false,
          bridge_ready: true,
          paused: false,
          reason: parsed.error,
          summary: `递物参数无效:${parsed.error}`,
          snapshot: this.getSnapshot(),
        };
      }
      this.activity = `递物:${parsed.targetPlayer}`;
      this._emitSnapshot();
      const outcome = await this._runBoundedGoal(actionType, timeoutMs, async (signal) => {
        return await executeGiveAction(bot, params, {
          signal,
          gotoGoal: (goal, sig) => this._gotoGoal(goal, sig),
          allowedPlayers: this.config.mcAllowedPlayers,
        });
      });
      this._refreshFromBot();
      this.activity = this.paused ? "已暂停" : this.connected ? "原地待命" : "待机";
      if (this.connected) this._emitSnapshot();
      if (outcome?.timedOut || outcome?.aborted) {
        return {
          ok: false,
          accepted: true,
          bridge_ready: true,
          paused: false,
          reason: outcome.timedOut ? "mc_goal_timeout" : "mc_goal_cancelled",
          summary: outcome.timedOut ? "递物超时" : "递物已取消",
          snapshot: this.getSnapshot(),
        };
      }
      if (outcome?.ok) {
        this._emitMilestone("mc_goal_completed", "normal", {
          action_type: "give",
          target_player: outcome.target_player,
          item_name: outcome.item_name,
          given_count: outcome.given_count,
          summary: outcome.summary,
        });
      }
      return {
        ok: Boolean(outcome?.ok),
        accepted: true,
        bridge_ready: true,
        paused: false,
        ...outcome,
        snapshot: this.getSnapshot(),
      };
    }

    throw new ProtocolError("unsupported_mc_action");
  }
}
