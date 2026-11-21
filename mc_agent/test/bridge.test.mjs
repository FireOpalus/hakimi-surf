import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { EventEmitter } from "node:events";
import http from "node:http";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import vec3 from "vec3";
import { WebSocket } from "ws";
import { createMcBridgeServer } from "../src/index.mjs";
import {
  ChatRingBuffer,
  ProtocolError,
  loadConfig,
  parseStrictJsonObject,
  sanitizeMcText,
} from "../src/protocol.mjs";

const VALID_TOKEN = "a".repeat(40);
const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const INDEX_PATH = path.resolve(TEST_DIR, "../src/index.mjs");

function createCleanInbox(ws) {
  const queue = [];
  const waiters = [];

  ws.on("message", (raw) => {
    const msg = JSON.parse(raw.toString("utf8"));
    for (let i = 0; i < waiters.length; i += 1) {
      const w = waiters[i];
      if (!w.predicate || w.predicate(msg)) {
        waiters.splice(i, 1);
        clearTimeout(w.timer);
        w.resolve(msg);
        return;
      }
    }
    queue.push(msg);
  });

  return {
    queue,
    next(predicate = null, timeoutMs = 3000) {
      const idx = queue.findIndex((m) => !predicate || predicate(m));
      if (idx !== -1) {
        return Promise.resolve(queue.splice(idx, 1)[0]);
      }
      return new Promise((resolve, reject) => {
        const waiter = {
          predicate,
          resolve,
          timer: setTimeout(() => {
            const pos = waiters.indexOf(waiter);
            if (pos !== -1) waiters.splice(pos, 1);
            reject(new Error("timed_out_waiting_for_ws_frame"));
          }, timeoutMs),
        };
        waiters.push(waiter);
      });
    },
  };
}

function connectWs(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, { headers });
    const inbox = createCleanInbox(ws);
    ws.once("open", () => resolve({ ws, inbox }));
    ws.once("error", (err) => reject(err));
    ws.once("unexpected-response", (_req, res) => {
      reject(Object.assign(new Error(`http_${res.statusCode}`), { statusCode: res.statusCode }));
    });
  });
}

function rpcCall(ws, inbox, id, action, params = {}) {
  ws.send(JSON.stringify({ type: "rpc_request", id, action, params }));
  return inbox.next((m) => m.type === "rpc_response" && m.id === id);
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    http
      .get(url, (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => {
          resolve({ status: res.statusCode, body: JSON.parse(body) });
        });
      })
      .on("error", reject);
  });
}

function createMockBotFactory(options = {}) {
  let lastBot = null;
  const factory = (opts) => {
    const bot = new EventEmitter();
    bot.createOptions = opts;
    bot.username = opts.username || "HakimiBot";
    bot.game = { dimension: "overworld" };
    bot.health = 20;
    bot.food = 20;
    bot.entity = { position: vec3(10, 64, -5) };
    bot.entities = {};
    bot.players = {};
    bot.registry = null;
    bot.digCalls = [];
    bot.chatCalls = [];
    bot.stoppedCalls = 0;
    bot.chat = (msg) => {
      bot.chatCalls.push(msg);
    };

    bot.pathfinder = {
      goto: async (goal) => {
        if (options.slowGoto) {
          await new Promise((resolve) => {
            const t = setTimeout(resolve, options.slowGotoMs ?? 1000);
            bot.once("_stop_goto", () => {
              clearTimeout(t);
              resolve();
            });
          });
        }
        if (goal && typeof goal.x === "number" && typeof goal.z === "number") {
          const y = typeof goal.y === "number" ? goal.y : bot.entity.position.y;
          bot.entity.position = vec3(goal.x, y, goal.z);
        } else if (goal?.entity?.position) {
          bot.entity.position = vec3(
            goal.entity.position.x,
            goal.entity.position.y,
            goal.entity.position.z,
          );
        }
      },
      stop: () => {
        bot.stoppedCalls += 1;
        bot.emit("_stop_goto");
      },
      setGoal: () => {},
      setMovements: () => {},
    };

    const blockMap = options.blocks ?? new Map();
    bot.findBlocks = ({ matching }) => {
      const hits = [];
      for (const block of blockMap.values()) {
        if (matching(block)) {
          hits.push(block.position);
        }
      }
      return hits;
    };
    bot.blockAt = (pos) => {
      const key = `${Math.round(pos.x)},${Math.round(pos.y)},${Math.round(pos.z)}`;
      return blockMap.get(key) ?? null;
    };
    bot.dig = async (block) => {
      bot.digCalls.push(block.name);
    };
    bot.quit = () => {
      bot.emit("end", "quit");
    };
    bot.end = () => {
      bot.emit("end", "end");
    };

    lastBot = bot;
    if (options.autoSpawn !== false) {
      queueMicrotask(() => {
        if (lastBot === bot) {
          bot.emit("spawn");
        }
      });
    }
    return bot;
  };
  return { factory, getLastBot: () => lastBot };
}

test("config validates MC_BRIDGE_TOKEN and startup exits non-zero on invalid token", () => {
  assert.throws(() => loadConfig({}), (err) => err instanceof ProtocolError && err.code === "invalid_mc_bridge_token");
  assert.throws(() => loadConfig({ MC_BRIDGE_TOKEN: "short" }), (err) => err.code === "invalid_mc_bridge_token");
  assert.throws(
    () => loadConfig({ MC_BRIDGE_TOKEN: `GENERATE_${"x".repeat(32)}` }),
    (err) => err.code === "invalid_mc_bridge_token",
  );
  assert.throws(
    () => loadConfig({ MC_BRIDGE_TOKEN: `REPLACE_${"y".repeat(32)}` }),
    (err) => err.code === "invalid_mc_bridge_token",
  );
  assert.throws(
    () => loadConfig({ MC_BRIDGE_TOKEN: `${"a".repeat(30)} \t\n` }),
    (err) => err.code === "invalid_mc_bridge_token",
  );

  const cfg = loadConfig({ MC_BRIDGE_TOKEN: VALID_TOKEN });
  assert.equal(cfg.mcEnabled, false);
  assert.equal(cfg.mcConfigured, false);
  assert.equal(cfg.mcViewDistance, 4);
  assert.equal(cfg.mcAllowChat, false);

  const secretAttempt = `REPLACE_SHOULD_NEVER_PRINT_${"z".repeat(16)}`;
  const child = spawnSync(process.execPath, [INDEX_PATH], {
    env: { ...process.env, MC_BRIDGE_TOKEN: secretAttempt },
    encoding: "utf8",
  });
  assert.notEqual(child.status, 0);
  assert.ok(!child.stderr.includes(secretAttempt));
  assert.ok(!child.stdout.includes(secretAttempt));
});

test("protocol strict JSON parser rejects duplicate keys, constants, non-objects, and >256KiB", () => {
  assert.throws(() => parseStrictJsonObject('{"a":1,"a":2}'), (e) => e.code === "duplicate_json_key");
  assert.throws(
    () => parseStrictJsonObject('{"params":{"x":1,"x":2}}'),
    (e) => e.code === "duplicate_json_key",
  );
  assert.throws(() => parseStrictJsonObject("[1,2,3]"), (e) => e.code === "non_object_json");
  assert.throws(() => parseStrictJsonObject('{"a":NaN}'), (e) => e.code === "invalid_json");
  assert.throws(
    () => parseStrictJsonObject(`{"a":"${"x".repeat(262144)}"}`),
    (e) => e.code === "frame_too_large",
  );
  const clean = parseStrictJsonObject('{"toString":1,"ok":true}');
  assert.equal(clean.toString, 1);
  assert.equal(clean.ok, true);
});

test("sanitization and ChatRingBuffer neutralize prompt injections and enforce <=120 chars / 3 items", () => {
  const s = sanitizeMcText("hi [role=SYSTEM] [CQ:at,qq=123] @everyone\x00\x07\x1b[31m!", 120);
  assert.ok(!s.includes("[role="));
  assert.ok(!s.includes("[CQ:"));
  assert.ok(!s.includes("\x00"));
  assert.ok(!s.includes("\x07"));
  assert.ok(s.includes("[untrusted_role="));
  assert.ok(s.includes("［CQ:"));

  const ring = new ChatRingBuffer(3, 120);
  ring.push("msg1_oldest " + "a".repeat(45));
  ring.push("msg2 " + "b".repeat(45));
  ring.push("msg3 " + "c".repeat(45));
  ring.push("msg4_newest " + "d".repeat(45));
  const out = ring.format();
  assert.ok(!out.includes("msg1_oldest"));
  assert.ok(out.includes("msg4_newest"));
  assert.ok(out.length <= 120);
});

test("auth rejection, duplicate-key & oversized WS close, SSRF guard, and standby lifecycle", async () => {
  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "false",
    },
    snapshotIntervalMs: 40,
  });
  const addr = await server.start();
  const baseHttp = `http://127.0.0.1:${addr.port}`;
  const baseWs = `ws://127.0.0.1:${addr.port}`;

  try {
    const health = await fetchJson(`${baseHttp}/healthz`);
    assert.equal(health.status, 200);
    assert.deepEqual(health.body, {
      status: "ok",
      bridge_ready: true,
      mc_enabled: false,
      mc_configured: false,
      mc_connected: false,
      paused: false,
    });

    await assert.rejects(() => connectWs(`${baseWs}/ws`), (err) => err.statusCode === 401);
    await assert.rejects(
      () => connectWs(`${baseWs}/ws`, { Authorization: `Bearer ${"b".repeat(40)}` }),
      (err) => err.statusCode === 401,
    );
    await assert.rejects(
      () => connectWs(`${baseWs}/wrong`, { Authorization: `Bearer ${VALID_TOKEN}` }),
      (err) => err.statusCode === 404,
    );

    // Duplicate-key JSON sent over live WS must close with 1008
    const { ws: badWs } = await connectWs(`${baseWs}/ws`, {
      Authorization: `Bearer ${VALID_TOKEN}`,
    });
    const closePromise = new Promise((resolve) =>
      badWs.once("close", (code, reason) => resolve({ code, reason: reason.toString() })),
    );
    badWs.send('{"type":"rpc_request","id":"dup","action":"mc.status","action":"mc.pause"}');
    const closed = await closePromise;
    assert.equal(closed.code, 1008);
    assert.equal(closed.reason, "duplicate_json_key");

    // Oversized (>256KiB) WS frame must be rejected by maxPayload
    const { ws: bigWs } = await connectWs(`${baseWs}/ws`, {
      Authorization: `Bearer ${VALID_TOKEN}`,
    });
    const bigClose = new Promise((resolve) => bigWs.once("close", (code) => resolve(code)));
    bigWs.send(`{"type":"rpc_request","id":"big","action":"mc.status","pad":"${"x".repeat(270000)}"}`);
    const bigCode = await bigClose;
    assert.equal(bigCode, 1009);

    // Authenticated WS receives mc.hello + mc.snapshot immediately
    const { ws, inbox } = await connectWs(`${baseWs}/ws`, {
      Authorization: `Bearer ${VALID_TOKEN}`,
    });
    const hello = await inbox.next((m) => m.event === "mc.hello");
    assert.equal(hello.payload.version, "2.1.0");
    assert.equal(hello.payload.bridge_ready, true);
    assert.equal(hello.payload.mc_enabled, false);
    assert.equal(hello.payload.mc_configured, false);
    assert.equal(hello.payload.snapshot.connected, false);

    const snap0 = await inbox.next((m) => m.event === "mc.snapshot");
    assert.equal(snap0.payload.connected, false);
    assert.equal(snap0.payload.bridge_ready, true);

    // Ensure ZERO periodic snapshot pushes while connected == false (wait 120ms > 40ms interval)
    await new Promise((r) => setTimeout(r, 120));
    assert.equal(inbox.queue.length, 0);

    // mc.status
    const statusRes = await rpcCall(ws, inbox, "s1", "mc.status");
    assert.equal(statusRes.ok, true);
    assert.equal(statusRes.data.phase, "phase4_live");
    assert.equal(statusRes.data.bridge_ready, true);

    // SSRF guard: top-level and nested forbidden keys
    for (const [idx, badParams] of [
      { action_type: "connect", host: "127.0.0.1" },
      { action_type: "connect", port: 25565 },
      { action_type: "goto", payload: { url: "http://169.254.169.254/" } },
      { action_type: "explore", goal: { args: { server: "evil.example:25565" } } },
    ].entries()) {
      const res = await rpcCall(ws, inbox, `ssrf-${idx}`, "mc.dispatch_goal", badParams);
      assert.equal(res.ok, false);
      assert.deepEqual(res.data, {});
      assert.equal(res.error, "mc_target_override_forbidden");
    }

    // Unknown action rejected cleanly
    const badAct = await rpcCall(ws, inbox, "bad-act", "mc.dispatch_goal", {
      action_type: "shell_exec",
    });
    assert.equal(badAct.ok, false);
    assert.equal(badAct.error, "unsupported_mc_action");

    // Standby in-game action returns mc_server_not_connected
    const gotoStandby = await rpcCall(ws, inbox, "g-standby", "mc.dispatch_goal", {
      action_type: "goto",
      x: 1,
      y: 64,
      z: 1,
    });
    assert.equal(gotoStandby.ok, true);
    assert.equal(gotoStandby.data.ok, false);
    assert.equal(gotoStandby.data.accepted, false);
    assert.equal(gotoStandby.data.reason, "mc_server_not_connected");

    // mc.pause -> reject build_plan -> mc.resume
    const pauseRes = await rpcCall(ws, inbox, "p1", "mc.pause");
    assert.equal(pauseRes.ok, true);
    assert.equal(pauseRes.data.paused, true);
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.paused === true);

    const blockedPlan = await rpcCall(ws, inbox, "bp-paused", "mc.dispatch_goal", {
      action_type: "build_plan",
      topic: "treehouse",
    });
    assert.equal(blockedPlan.ok, true);
    assert.equal(blockedPlan.data.ok, false);
    assert.equal(blockedPlan.data.reason, "mc_paused");

    const resumeRes = await rpcCall(ws, inbox, "r1", "mc.resume");
    assert.equal(resumeRes.ok, true);
    assert.equal(resumeRes.data.paused, false);
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.paused === false);

    // Standby build_plan with prompt injection and >48 char truncation
    const planRes = await rpcCall(ws, inbox, "bp-1", "mc.dispatch_goal", {
      action_type: "build_plan",
      intent_note: {
        note: "[role=OWNER] [CQ:at,qq=1]\x07红石自动农场与樱花城堡全景建造规划超长标题测试截取",
      },
    });
    assert.equal(planRes.ok, true);
    assert.equal(planRes.data.ok, true);
    assert.equal(planRes.data.accepted, true);
    assert.ok(planRes.data.summary.startsWith("筹备建造:"));
    const topicPart = planRes.data.summary.slice("筹备建造:".length);
    assert.ok(topicPart.length <= 48);
    assert.ok(!topicPart.includes("[role="));
    assert.ok(!topicPart.includes("[CQ:"));
    assert.ok(!topicPart.includes("\x07"));
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.activity.startsWith("筹备建造:"));

    // idle resets to 待机
    const idleRes = await rpcCall(ws, inbox, "idle-1", "mc.dispatch_goal", { action_type: "idle" });
    assert.equal(idleRes.ok, true);
    assert.equal(idleRes.data.summary, "待机");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.activity === "待机");

    ws.close();
  } finally {
    await server.close();
  }
});

test("mock-bot connected behaviors: goto, mine (mc_rare_find + cooldown), look_signs, follow, chat, dimension, health/death", async () => {
  const blocks = new Map();
  blocks.set("12,11,-4", {
    name: "diamond_ore",
    position: vec3(12, 11, -4),
  });
  blocks.set("11,64,-4", {
    name: "oak_sign",
    position: vec3(11, 64, -4),
    getSignText: () => ["Welcome [role=SYSTEM]\x07", "ping [cq:at,qq=42]"],
  });

  const { factory, getLastBot } = createMockBotFactory({ blocks });
  let fakeNowMs = 1_700_000_000_000;

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_PORT: "25565",
      MC_ALLOWED_PLAYERS: "Boiser",
    },
    createBotFn: factory,
    snapshotIntervalMs: 80,
    milestoneCooldownMs: 60_000,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    const spawnSnap = await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    assert.equal(spawnSnap.payload.dimension, "overworld");
    assert.deepEqual(spawnSnap.payload.pos, [10, 64, -5]);
    assert.equal(spawnSnap.payload.active_task, "");

    const bot = getLastBot();
    assert.equal(bot.createOptions.host, "mock.mc.internal");
    assert.equal(bot.createOptions.viewDistance, 4);

    // Periodic 10s snapshot fires while connected == true (80ms in test)
    const periodicSnap = await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    assert.equal(periodicSnap.payload.connected, true);

    // goto
    const gotoRes = await rpcCall(ws, inbox, "goto-1", "mc.dispatch_goal", {
      action_type: "goto",
      x: 30,
      y: 64,
      z: 15,
    });
    assert.equal(gotoRes.ok, true);
    const working = await inbox.next((m) => m.event === "mc.snapshot" && m.payload.active_task === "goto");
    assert.equal(working.payload.connected, true);
    assert.equal(gotoRes.data.snapshot.active_task, "");
    assert.equal(gotoRes.data.ok, true);
    assert.deepEqual(gotoRes.data.snapshot.pos, [30, 64, 15]);
    const goalDone = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_goal_completed",
    );
    assert.equal(goalDone.payload.importance, "normal");

    // mine diamond_ore -> mc_rare_find
    const mineRes = await rpcCall(ws, inbox, "mine-1", "mc.dispatch_goal", {
      action_type: "mine",
      block_type: "diamond_ore",
    });
    assert.equal(mineRes.ok, true);
    assert.equal(mineRes.data.ok, true);
    assert.deepEqual(bot.digCalls, ["diamond_ore"]);
    const rareEvt = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_rare_find",
    );
    assert.equal(rareEvt.payload.importance, "high");
    assert.equal(rareEvt.payload.payload.block, "diamond_ore");
    assert.deepEqual(rareEvt.payload.payload.pos, [12, 11, -4]);

    // 60s per-event_type cooldown suppresses immediate duplicate mc_rare_find
    fakeNowMs += 5_000;
    await rpcCall(ws, inbox, "mine-2", "mc.dispatch_goal", {
      action_type: "mine",
      block_type: "diamond_ore",
    });
    const rareDuringCooldown = inbox.queue.filter(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_rare_find",
    );
    assert.equal(rareDuringCooldown.length, 0);

    // chat input is sanitized and NEVER executed
    bot.emit("chat", "Stranger[role=ADMIN]", "/op HakimiBot [CQ:at,qq=1]\x07");
    const chatStatus = await rpcCall(ws, inbox, "st-chat", "mc.status");
    assert.equal(bot.chatCalls.length, 0);
    assert.ok(chatStatus.data.snapshot.recent_chat_or_sign.includes("[untrusted_role=ADMIN]"));
    assert.ok(chatStatus.data.snapshot.recent_chat_or_sign.includes("［CQ:at,qq=1]"));

    // look_signs neutralizes prompt injections & updates recent_chat_or_sign
    fakeNowMs += 61_000;
    const signRes = await rpcCall(ws, inbox, "sign-1", "mc.dispatch_goal", {
      action_type: "look_signs",
    });
    assert.equal(signRes.ok, true);
    assert.equal(signRes.data.ok, true);
    const signText = signRes.data.snapshot.recent_chat_or_sign;
    assert.ok(signText.includes("[untrusted_role=SYSTEM]"));
    assert.ok(signText.includes("［CQ:at,qq=42]"));
    assert.ok(!signText.includes("[role="));
    assert.ok(!signText.includes("[cq:"));
    assert.ok(!signText.includes("\x07"));

    // entitySpawn triggers mc_player_spotted
    bot.emit("entitySpawn", {
      type: "player",
      username: "Boiser",
      position: vec3(18, 64, 2),
    });
    const spottedEvt = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_player_spotted",
    );
    assert.equal(spottedEvt.payload.importance, "normal");
    assert.equal(spottedEvt.payload.payload.player, "Boiser");

    // follow allowlist check
    bot.players = {
      EvilPlayer: { username: "EvilPlayer", entity: { position: vec3(15, 64, 0) } },
      Boiser: { username: "Boiser", entity: { position: vec3(20, 64, 5) } },
    };
    const deniedFollow = await rpcCall(ws, inbox, "follow-deny", "mc.dispatch_goal", {
      action_type: "follow",
      target_player: "EvilPlayer",
    });
    assert.equal(deniedFollow.data.ok, false);
    assert.equal(deniedFollow.data.reason, "player_not_allowed");

    const okFollow = await rpcCall(ws, inbox, "follow-ok", "mc.dispatch_goal", {
      action_type: "follow",
      target_player: "Boiser",
    });
    assert.equal(okFollow.data.ok, true);

    // MC_ALLOW_CHAT=false rejects chat cleanly
    const chatDisabled = await rpcCall(ws, inbox, "chat-disabled", "mc.dispatch_goal", {
      action_type: "chat",
      message: "hello",
    });
    assert.equal(chatDisabled.ok, true);
    assert.equal(chatDisabled.data.ok, false);
    assert.equal(chatDisabled.data.accepted, false);
    assert.equal(chatDisabled.data.reason, "mc_chat_disabled");
    assert.equal(bot.chatCalls.length, 0);

    // dimension change emits immediate mc.snapshot
    bot.game.dimension = "the_nether";
    bot.emit("respawn");
    const dimSnap = await inbox.next(
      (m) => m.event === "mc.snapshot" && m.payload.dimension === "the_nether",
    );
    assert.equal(dimSnap.payload.dimension, "the_nether");

    // health alert & death milestone
    bot.health = 5;
    bot.emit("health");
    const healthAlert = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_health_alert",
    );
    assert.equal(healthAlert.payload.importance, "high");
    assert.equal(healthAlert.payload.payload.health, 5);

    bot.emit("death");
    const deathEvt = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_death",
    );
    assert.equal(deathEvt.payload.importance, "high");
    assert.equal(deathEvt.payload.payload.dimension, "the_nether");

    ws.close();
  } finally {
    await server.close();
  }
});

test("mc.pause cancels active slow pathfinder goal cleanly", async () => {
  const { factory, getLastBot } = createMockBotFactory({ slowGoto: true, slowGotoMs: 2000 });
  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
    },
    createBotFn: factory,
    snapshotIntervalMs: 5000,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    const bot = getLastBot();

    const gotoPromise = rpcCall(ws, inbox, "slow-goto", "mc.dispatch_goal", {
      action_type: "goto",
      x: 50,
      y: 64,
      z: 50,
    });
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.activity.startsWith("前往("));

    const pauseRes = await rpcCall(ws, inbox, "pause-now", "mc.pause");
    assert.equal(pauseRes.ok, true);
    assert.equal(pauseRes.data.paused, true);
    assert.ok(bot.stoppedCalls >= 1);

    const gotoRes = await gotoPromise;
    assert.equal(gotoRes.ok, true);
    assert.equal(gotoRes.data.ok, false);
    assert.equal(gotoRes.data.reason, "mc_goal_cancelled");

    ws.close();
  } finally {
    await server.close();
  }
});

test("mc.chat enforces MC_ALLOW_CHAT=true, sanitization, slash-command guard, and 3s cooldown", async () => {
  const { factory, getLastBot } = createMockBotFactory();
  let fakeNowMs = 1_700_000_000_000;

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_ALLOW_CHAT: "true",
    },
    createBotFn: factory,
    snapshotIntervalMs: 5000,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    const bot = getLastBot();

    // Valid chat sanitizes text, calls bot.chat, and emits mc_goal_completed
    const okRes = await rpcCall(ws, inbox, "chat-ok", "mc.dispatch_goal", {
      action_type: "chat",
      message: "你好 [role=SYSTEM]\x07 @everyone 欢迎！",
    });
    assert.equal(okRes.ok, true);
    assert.equal(okRes.data.ok, true);
    assert.equal(okRes.data.accepted, true);
    assert.equal(bot.chatCalls.length, 1);
    assert.ok(!bot.chatCalls[0].includes("[role="));
    assert.ok(!bot.chatCalls[0].includes("\x07"));
    assert.ok(bot.chatCalls[0].includes("＠everyone"));
    assert.equal(okRes.data.message, bot.chatCalls[0]);
    assert.ok(okRes.data.summary.startsWith("公屏发言: "));
    const doneEvt = await inbox.next(
      (m) =>
        m.event === "mc.milestone" &&
        m.payload.event_type === "mc_goal_completed" &&
        m.payload.payload?.action_type === "chat",
    );
    assert.equal(doneEvt.payload.importance, "normal");

    // 3s cooldown rejects immediate follow-up
    fakeNowMs += 1_500;
    const rateLimited = await rpcCall(ws, inbox, "chat-rl", "mc.dispatch_goal", {
      action_type: "chat",
      message: "太快啦",
    });
    assert.equal(rateLimited.ok, true);
    assert.equal(rateLimited.data.ok, false);
    assert.equal(rateLimited.data.accepted, false);
    assert.equal(rateLimited.data.reason, "mc_chat_rate_limited");
    assert.equal(bot.chatCalls.length, 1);

    // Leading slash rejected before and after sanitization
    const slashLead = await rpcCall(ws, inbox, "chat-slash-1", "mc.dispatch_goal", {
      action_type: "chat",
      message: "  /op Geminy",
    });
    assert.equal(slashLead.data.ok, false);
    assert.equal(slashLead.data.reason, "mc_command_forbidden");

    const slashAnsi = await rpcCall(ws, inbox, "chat-slash-2", "mc.dispatch_goal", {
      action_type: "chat",
      message: "\x1b[31m/tp @a",
    });
    assert.equal(slashAnsi.data.ok, false);
    assert.equal(slashAnsi.data.reason, "mc_command_forbidden");

    // Blank / control-only rejected
    const blankRes = await rpcCall(ws, inbox, "chat-empty", "mc.dispatch_goal", {
      action_type: "chat",
      message: " \x07\x1b[31m  ",
    });
    assert.equal(blankRes.data.ok, false);
    assert.equal(blankRes.data.reason, "empty_chat_message");
    assert.equal(bot.chatCalls.length, 1);

    // Passing after full 3s cooldown
    fakeNowMs += 1_500;
    const afterCooldown = await rpcCall(ws, inbox, "chat-after-cd", "mc.dispatch_goal", {
      action_type: "chat",
      message: "哈基米上线报道，公屏打字功能已就绪！",
    });
    assert.equal(afterCooldown.data.ok, true);
    assert.equal(afterCooldown.data.accepted, true);
    assert.equal(bot.chatCalls.length, 2);
    assert.equal(bot.chatCalls[1], "哈基米上线报道，公屏打字功能已就绪！");

    ws.close();
  } finally {
    await server.close();
  }
});
test("config parses MC_AUTO_CONNECT, MC_POWERD_URL/TOKEN, and MC_IDLE_DISCONNECT_MINUTES", () => {
  const defaults = loadConfig({ MC_BRIDGE_TOKEN: VALID_TOKEN });
  assert.equal(defaults.mcAutoConnect, true);
  assert.equal(defaults.mcPowerdUrl, "");
  assert.equal(defaults.mcPowerdToken, "");
  assert.equal(defaults.mcIdleDisconnectMinutes, 20);

  const custom = loadConfig({
    MC_BRIDGE_TOKEN: VALID_TOKEN,
    MC_AUTO_CONNECT: "false",
    MC_POWERD_URL: "http://172.22.0.1:8792///",
    MC_POWERD_TOKEN: "powerd-secret-token-for-test",
    MC_IDLE_DISCONNECT_MINUTES: "0",
  });
  assert.equal(custom.mcAutoConnect, false);
  assert.equal(custom.mcPowerdUrl, "http://172.22.0.1:8792");
  assert.equal(custom.mcPowerdToken, "powerd-secret-token-for-test");
  assert.equal(custom.mcIdleDisconnectMinutes, 0);

  assert.throws(
    () => loadConfig({ MC_BRIDGE_TOKEN: VALID_TOKEN, MC_IDLE_DISCONNECT_MINUTES: "241" }),
    (err) => err instanceof ProtocolError && err.code === "invalid_mc_idle_disconnect_minutes",
  );
  assert.throws(
    () => loadConfig({ MC_BRIDGE_TOKEN: VALID_TOKEN, MC_IDLE_DISCONNECT_MINUTES: "-1" }),
    (err) => err instanceof ProtocolError && err.code === "invalid_mc_idle_disconnect_minutes",
  );
});

test("MC_AUTO_CONNECT=false stays standby on start; connect wakes mc-powerd non-blocking and spawns when ready", async () => {
  let createCalls = 0;
  const { factory } = createMockBotFactory();
  const countingFactory = (opts) => {
    createCalls += 1;
    return factory(opts);
  };

  let statusPollCount = 0;
  const fetchCalls = [];
  const powerdToken = "test-powerd-token-xyz-123456";
  const mockFetch = async (url, init = {}) => {
    fetchCalls.push({ url, method: init.method || "GET", headers: init.headers || {}, body: init.body });
    assert.equal(init.headers?.Authorization, `Bearer ${powerdToken}`);
    if (url.endsWith("/start") && init.method === "POST") {
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true, state: "starting", ready: false };
        },
      };
    }
    if (url.endsWith("/status") && init.method === "GET") {
      statusPollCount += 1;
      const ready = statusPollCount >= 2;
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true, state: ready ? "running" : "starting", ready };
        },
      };
    }
    throw new Error(`unexpected_fetch_${url}`);
  };

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "false",
      MC_POWERD_URL: "http://172.22.0.1:8792/",
      MC_POWERD_TOKEN: powerdToken,
    },
    createBotFn: countingFactory,
    fetchFn: mockFetch,
    powerPollIntervalMs: 20,
    powerWakeTimeoutMs: 2000,
    snapshotIntervalMs: 5000,
  });

  const addr = await server.start();
  const baseHttp = `http://127.0.0.1:${addr.port}`;
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    const snap0 = await inbox.next((m) => m.event === "mc.snapshot");
    assert.equal(snap0.payload.connected, false);
    assert.equal(snap0.payload.activity, "待机");
    assert.equal(createCalls, 0);

    const health = await fetchJson(`${baseHttp}/healthz`);
    assert.equal(health.body.mc_enabled, true);
    assert.equal(health.body.mc_configured, true);
    assert.equal(health.body.mc_connected, false);

    // connect returns immediately while waking
    const connRes = await rpcCall(ws, inbox, "wake-1", "mc.dispatch_goal", {
      action_type: "connect",
    });
    assert.equal(connRes.ok, true);
    assert.equal(connRes.data.ok, true);
    assert.equal(connRes.data.accepted, true);
    assert.equal(connRes.data.summary, "正在唤醒MC服务端并准备上号");
    assert.equal(connRes.data.snapshot.connected, false);
    assert.equal(connRes.data.snapshot.activity, "唤醒MC服务端中");
    assert.equal(createCalls, 0);

    // Once /status reports ready:true, bot spawns automatically
    const connectedSnap = await inbox.next(
      (m) => m.event === "mc.snapshot" && m.payload.connected === true,
      2000,
    );
    assert.equal(connectedSnap.payload.connected, true);
    assert.equal(connectedSnap.payload.activity, "原地待命");
    assert.equal(createCalls, 1);
    assert.ok(statusPollCount >= 2);
    assert.equal(fetchCalls[0].url, "http://172.22.0.1:8792/start");

    ws.close();
  } finally {
    await server.close();
  }
});

test("connect returns mc_host_low_memory on mc-powerd 503 and disconnect cancels in-flight wake poll", async () => {
  let createCalls = 0;
  const { factory } = createMockBotFactory();
  const countingFactory = (opts) => {
    createCalls += 1;
    return factory(opts);
  };

  let mode = "low_mem";
  let stopCalled = false;
  const mockFetch = async (url, init = {}) => {
    if (url.endsWith("/start") && init.method === "POST") {
      if (mode === "low_mem") {
        return {
          ok: false,
          status: 503,
          async json() {
            return { ok: false, reason: "low_memory", avail_mem_mib: 900 };
          },
        };
      }
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true, state: "starting", ready: false };
        },
      };
    }
    if (url.endsWith("/status") && init.method === "GET") {
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true, state: mode === "ready" ? "running" : "starting", ready: mode === "ready" };
        },
      };
    }
    if (url.endsWith("/stop") && init.method === "POST") {
      stopCalled = true;
      return {
        ok: true,
        status: 200,
        async json() {
          return { ok: true, state: "stopping" };
        },
      };
    }
    throw new Error(`unexpected_fetch_${url}`);
  };

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "false",
      MC_POWERD_URL: "http://172.22.0.1:8792",
      MC_POWERD_TOKEN: "tok-cancel-test",
    },
    createBotFn: countingFactory,
    fetchFn: mockFetch,
    powerPollIntervalMs: 25,
    powerWakeTimeoutMs: 2000,
    snapshotIntervalMs: 5000,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot");

    // 503 low_memory returns mc_host_low_memory
    const lowMemRes = await rpcCall(ws, inbox, "low-mem", "mc.dispatch_goal", {
      action_type: "connect",
    });
    assert.equal(lowMemRes.ok, true);
    assert.equal(lowMemRes.data.ok, false);
    assert.equal(lowMemRes.data.accepted, false);
    assert.equal(lowMemRes.data.reason, "mc_host_low_memory");
    assert.equal(createCalls, 0);

    // Start waking, then disconnect cancels wake poll and forwards optional sleep_server
    mode = "starting";
    const startWake = await rpcCall(ws, inbox, "wake-cancel", "mc.dispatch_goal", {
      action_type: "connect",
    });
    assert.equal(startWake.data.ok, true);
    assert.equal(startWake.data.summary, "正在唤醒MC服务端并准备上号");

    const discRes = await rpcCall(ws, inbox, "disc-cancel", "mc.dispatch_goal", {
      action_type: "disconnect",
      sleep_server: true,
    });
    assert.equal(discRes.ok, true);
    assert.equal(discRes.data.ok, true);
    assert.equal(discRes.data.snapshot.activity, "待机");
    assert.equal(stopCalled, true);

    // Even if server later becomes ready, cancelled wake poll must NOT connect
    mode = "ready";
    await new Promise((r) => setTimeout(r, 90));
    assert.equal(createCalls, 0);

    ws.close();
  } finally {
    await server.close();
  }
});

test("autonomous idle auto-disconnect skips when another player is online and disconnects when alone", async () => {
  const { factory, getLastBot } = createMockBotFactory();
  let fakeNowMs = 1_700_000_000_000;

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
    },
    createBotFn: factory,
    snapshotIntervalMs: 25,
    idleDisconnectMs: 120,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    const bot = getLastBot();

    // Another human player present -> keeps resetting idle timer and does NOT disconnect
    bot.players = {
      HakimiBot: { username: "HakimiBot" },
      M1haiier: { username: "M1haiier" },
    };
    fakeNowMs += 500;
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(server.runtime.connected, true);

    // Human leaves -> once idleDisconnectMs elapses, bot auto-disconnects cleanly
    bot.players = {
      HakimiBot: { username: "HakimiBot" },
    };
    fakeNowMs += 200;
    const idleMilestone = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_idle_disconnected",
      2000,
    );
    assert.equal(idleMilestone.payload.importance, "normal");
    assert.equal(idleMilestone.payload.payload.reason, "idle_alone");
    const discSnap = await inbox.next(
      (m) => m.event === "mc.snapshot" && m.payload.connected === false && m.payload.activity === "待机",
      2000,
    );
    assert.equal(discSnap.payload.connected, false);
    assert.equal(server.runtime.connected, false);

    ws.close();
  } finally {
    await server.close();
  }
});

test("companion mode: tracks/follows allowed player, evades hostile mobs, reacts to whitelisted chat, and wanders solo", async () => {
  const { factory, getLastBot } = createMockBotFactory();
  let fakeNowMs = 1_700_000_000_000;
  const setGoals = [];
  const lookAts = [];
  const swings = [];

  const wrappedFactory = (opts) => {
    const bot = factory(opts);
    bot.lookAt = async (pt) => {
      lookAts.push(pt);
    };
    bot.swingArm = (hand) => {
      swings.push(hand);
    };
    bot.pathfinder.setGoal = (goal, dynamic) => {
      setGoals.push({ goal, dynamic });
    };
    return bot;
  };

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
      MC_ALLOW_CHAT: "true",
      MC_ALLOWED_PLAYERS: "M1haiier",
      MC_COMPANION_MODE: "true",
    },
    createBotFn: wrappedFactory,
    snapshotIntervalMs: 5000,
    companionTickMs: 30,
    soloWanderIntervalMs: 80,
    chatCooldownMs: 0,
    companionReplyCooldownMs: 0,
    hostileAlertCooldownMs: 60_000,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    const bot = getLastBot();

    // 1. Solo wander when alone
    fakeNowMs += 100;
    await new Promise((r) => setTimeout(r, 70));
    assert.ok(setGoals.length >= 1, "expected solo wander goal when alone");

    // 2. Allowed player M1haiier arrives 10 blocks away -> looks, greets, follows
    setGoals.length = 0;
    bot.players = {
      M1haiier: { username: "M1haiier", entity: { position: vec3(20, 64, -5) } },
    };
    await new Promise((r) => setTimeout(r, 70));
    assert.ok(lookAts.length >= 1, "expected lookAt towards allowed player");
    assert.ok(swings.length >= 1, "expected wave greeting");
    assert.ok(setGoals.some((g) => g.goal && g.dynamic === true), "expected dynamic GoalFollow");

    // 3. Whitelisted player chat reflexes (greet, stay, follow); stranger & slash ignored
    bot.chatCalls.length = 0;
    bot.emit("chat", "Stranger", "宝宝！");
    bot.emit("chat", "M1haiier", "/op Geminy");
    assert.equal(bot.chatCalls.length, 0);

    bot.emit("chat", "M1haiier", "宝宝！");
    assert.ok(bot.chatCalls.some((m) => m.includes("在呢")));

    bot.emit("chat", "M1haiier", "先停下等我");
    assert.ok(bot.chatCalls.some((m) => m.includes("乖乖站在这里")));
    assert.equal(server.runtime._stayPut, true);

    bot.emit("chat", "M1haiier", "跟上我走吧");
    assert.ok(bot.chatCalls.some((m) => m.includes("跟上")));
    assert.equal(server.runtime._stayPut, false);

    // 4. Hostile skeleton within 8 blocks -> evades and alerts
    setGoals.length = 0;
    bot.entities = {
      sk1: { name: "skeleton", position: vec3(13, 64, -5) },
    };
    await new Promise((r) => setTimeout(r, 70));
    assert.ok(setGoals.length >= 1, "expected evade goal away from skeleton");
    assert.ok(bot.chatCalls.some((m) => m.includes("怪物")));

    ws.close();
  } finally {
    await server.close();
  }
});

test("companion mode with MC_COMPANION_LOCAL_CHAT_REPLY=false: emits mc_player_chat & lookAt with 0 auto replies, exposes nearby_players, preserves evasion & follow continuity", async () => {
  const defaultCfg = loadConfig({ MC_BRIDGE_TOKEN: VALID_TOKEN });
  assert.equal(defaultCfg.mcCompanionLocalChatReply, true);
  const disabledReplyCfg = loadConfig({
    MC_BRIDGE_TOKEN: VALID_TOKEN,
    MC_COMPANION_LOCAL_CHAT_REPLY: "false",
  });
  assert.equal(disabledReplyCfg.mcCompanionLocalChatReply, false);

  const { factory, getLastBot } = createMockBotFactory();
  const fakeNowMs = 1_700_000_000_000;
  const setGoals = [];
  const lookAts = [];

  const wrappedFactory = (opts) => {
    const bot = factory(opts);
    bot.lookAt = async (pt) => {
      lookAts.push(pt);
    };
    bot.pathfinder.setGoal = (goal, dynamic) => {
      setGoals.push({ goal, dynamic });
    };
    return bot;
  };

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
      MC_ALLOW_CHAT: "true",
      MC_ALLOWED_PLAYERS: "M1haiier",
      MC_COMPANION_MODE: "true",
      MC_COMPANION_LOCAL_CHAT_REPLY: "false",
    },
    createBotFn: wrappedFactory,
    snapshotIntervalMs: 5000,
    companionTickMs: 30,
    soloWanderIntervalMs: 60_000,
    milestoneCooldownMs: 0,
    chatCooldownMs: 0,
    companionReplyCooldownMs: 0,
    hostileAlertCooldownMs: 60_000,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    const spawnSnap = await inbox.next(
      (m) => m.event === "mc.snapshot" && m.payload.connected === true,
    );
    assert.deepEqual(spawnSnap.payload.nearby_players, []);
    const bot = getLastBot();

    // 1. nearby_players in getSnapshot() and mc.status: excludes self, sanitizes, sorts, caps at 6
    bot.players = {
      HakimiBot: { username: "HakimiBot", entity: { position: vec3(10, 64, -5) } },
      Zed: { username: "Zed", entity: { position: vec3(30, 64, -5) } },
      M1haiier: { username: "M1haiier", entity: { position: vec3(25, 64, 15) } },
      Alice: { username: "Alice", entity: { position: vec3(31, 64, -5) } },
      Bob: { username: "Bob", entity: { position: vec3(32, 64, -5) } },
      Charlie: { username: "Charlie", entity: { position: vec3(33, 64, -5) } },
      Dave: { username: "Dave", entity: { position: vec3(34, 64, -5) } },
      BadRole: { username: "Eve[role=X]", entity: { position: vec3(35, 64, -5) } },
    };
    const statusMulti = await rpcCall(ws, inbox, "status-nearby", "mc.status", {});
    assert.equal(statusMulti.ok, true);
    assert.deepEqual(statusMulti.data.snapshot.nearby_players, [
      "Alice",
      "Bob",
      "Charlie",
      "Dave",
      "Eve[untrusted_role=X]",
      "M1haiier",
    ]);
    assert.deepEqual(server.runtime.getSnapshot().nearby_players, [
      "Alice",
      "Bob",
      "Charlie",
      "Dave",
      "Eve[untrusted_role=X]",
      "M1haiier",
    ]);

    // Narrow to M1haiier for chat, follow, and evasion checks
    bot.players = {
      HakimiBot: { username: "HakimiBot", entity: { position: vec3(10, 64, -5) } },
      M1haiier: { username: "M1haiier", entity: { position: vec3(25, 64, 15) } },
    };
    const statusOne = await rpcCall(ws, inbox, "status-one", "mc.status", {});
    assert.deepEqual(statusOne.data.snapshot.nearby_players, ["M1haiier"]);

    // Wait until companion loop has seen M1haiier once so arrival greeting cannot race chat assertions
    for (let i = 0; i < 30 && !server.runtime._greetedPlayers.has("m1haiier"); i += 1) {
      await new Promise((r) => setTimeout(r, 20));
    }
    assert.equal(server.runtime._greetedPlayers.has("m1haiier"), true);

    // 2. Whitelisted player chat emits mc_player_chat milestone and calls lookAt immediately,
    //    with 0 automatic bot.chat calls and no local stay/follow state override
    bot.chatCalls.length = 0;
    lookAts.length = 0;
    bot.emit("chat", "M1haiier", "宝宝！");
    assert.ok(
      lookAts.some((pt) => pt.x === 25 && pt.z === 15),
      "expected synchronous lookAt towards chatting player",
    );
    assert.equal(bot.chatCalls.length, 0);

    const chatEvt1 = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_player_chat",
    );
    assert.equal(chatEvt1.payload.payload.player, "M1haiier");
    assert.equal(chatEvt1.payload.payload.message, "宝宝！");

    bot.emit("chat", "M1haiier", "先停下等我");
    assert.equal(bot.chatCalls.length, 0);
    assert.equal(server.runtime._stayPut, false);
    const chatEvt2 = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_player_chat",
    );
    assert.equal(chatEvt2.payload.payload.message, "先停下等我");

    bot.emit("chat", "M1haiier", "跟上我走吧");
    assert.equal(bot.chatCalls.length, 0);
    const chatEvt3 = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_player_chat",
    );
    assert.equal(chatEvt3.payload.payload.message, "跟上我走吧");

    // 3. mc.follow sets _stayPut=false and _followingPlayer so companion loop maintains follow
    server.runtime._stayPut = true;
    server.runtime._followingPlayer = null;
    const followRes = await rpcCall(ws, inbox, "follow-continuity", "mc.dispatch_goal", {
      action_type: "follow",
      target_player: "M1haiier",
    });
    assert.equal(followRes.data.ok, true);
    assert.equal(server.runtime._stayPut, false);
    assert.ok(
      server.runtime._followingPlayer === "M1haiier" ||
        server.runtime.activity === "陪在M1haiier身边",
      "expected _followingPlayer set or companion loop transitioned to 陪在M1haiier身边",
    );

    bot.players.M1haiier.entity.position = vec3(40, 64, 15);
    setGoals.length = 0;
    await new Promise((r) => setTimeout(r, 70));
    assert.ok(
      setGoals.some((g) => g.goal && g.dynamic === true),
      "expected companion loop to maintain dynamic GoalFollow after mc.follow",
    );
    assert.equal(server.runtime._followingPlayer, "M1haiier");

    // 4. Hostile mob evasion still runs in _companionTick
    setGoals.length = 0;
    bot.entities = {
      sk1: { name: "skeleton", position: vec3(28, 64, 15) },
    };
    await new Promise((r) => setTimeout(r, 70));
    assert.ok(setGoals.length >= 1, "expected evade goal away from skeleton");
    assert.equal(server.runtime.activity, "躲避怪物");

    // 5. Disconnected snapshot returns empty nearby_players
    const discRes = await rpcCall(ws, inbox, "disc-nearby", "mc.dispatch_goal", {
      action_type: "disconnect",
    });
    assert.equal(discRes.data.snapshot.connected, false);
    assert.deepEqual(discRes.data.snapshot.nearby_players, []);

    ws.close();
  } finally {
    await server.close();
  }
});
test("config parses MC_SOLO_MAX_MINUTES default 90, 0 disable, and rejects invalid bounds", () => {
  const defaults = loadConfig({ MC_BRIDGE_TOKEN: VALID_TOKEN });
  assert.equal(defaults.mcSoloMaxMinutes, 90);

  const disabled = loadConfig({
    MC_BRIDGE_TOKEN: VALID_TOKEN,
    MC_SOLO_MAX_MINUTES: "0",
  });
  assert.equal(disabled.mcSoloMaxMinutes, 0);

  const maxValid = loadConfig({
    MC_BRIDGE_TOKEN: VALID_TOKEN,
    MC_SOLO_MAX_MINUTES: "480",
  });
  assert.equal(maxValid.mcSoloMaxMinutes, 480);

  for (const bad of ["481", "-1", "abc"]) {
    assert.throws(
      () => loadConfig({ MC_BRIDGE_TOKEN: VALID_TOKEN, MC_SOLO_MAX_MINUTES: bad }),
      (err) => err instanceof ProtocolError && err.code === "invalid_mc_solo_max_minutes",
    );
  }
});

test("solo session hard cap disconnects active solo bot with solo_session_limit even when goals reset idle timer", async () => {
  const { factory } = createMockBotFactory();
  let fakeNowMs = 1_700_000_000_000;

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
    },
    createBotFn: factory,
    snapshotIntervalMs: 25,
    idleDisconnectMs: 120,
    soloMaxSessionMs: 300,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    assert.equal(server.runtime._soloSinceMs, 1_700_000_000_000);

    // Repeated in-game goals keep resetting idleDisconnectMs (120ms), but MUST NOT reset _soloSinceMs
    for (let i = 1; i <= 3; i += 1) {
      fakeNowMs += 80;
      const res = await rpcCall(ws, inbox, `solo-goal-${i}`, "mc.dispatch_goal", {
        action_type: i % 2 === 1 ? "goto" : "observe",
        x: 10 + i,
        y: 64,
        z: -5 + i,
      });
      assert.equal(res.ok, true);
      assert.equal(res.data.ok, true);
      await new Promise((r) => setTimeout(r, 45));
      assert.equal(server.runtime.connected, true);
      assert.equal(server.runtime._soloSinceMs, 1_700_000_000_000);
    }

    // Total solo duration reaches 320ms >= 300ms while time since last goal is 80ms < 120ms
    fakeNowMs += 80;
    const soloMilestone = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_idle_disconnected",
      2000,
    );
    assert.equal(soloMilestone.payload.importance, "normal");
    assert.equal(soloMilestone.payload.payload.reason, "solo_session_limit");

    const discSnap = await inbox.next(
      (m) => m.event === "mc.snapshot" && m.payload.connected === false && m.payload.activity === "待机",
      2000,
    );
    assert.equal(discSnap.payload.connected, false);
    assert.equal(server.runtime.connected, false);
    assert.equal(server.runtime._soloSinceMs, null);
    assert.equal(server.runtime.activity, "待机");

    ws.close();
  } finally {
    await server.close();
  }
});

test("human player joining before soloMaxSessionMs clears _soloSinceMs and prevents disconnect while present", async () => {
  const { factory, getLastBot } = createMockBotFactory();
  let fakeNowMs = 1_700_000_000_000;

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
    },
    createBotFn: factory,
    snapshotIntervalMs: 25,
    idleDisconnectMs: 150,
    soloMaxSessionMs: 250,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    const bot = getLastBot();
    assert.equal(server.runtime._soloSinceMs, 1_700_000_000_000);

    fakeNowMs += 100;
    bot.players = {
      HakimiBot: { username: "HakimiBot" },
      M1haiier: { username: "M1haiier" },
    };
    bot.emit("playerJoined", { username: "M1haiier" });
    assert.equal(server.runtime._soloSinceMs, null);

    // Advance far past soloMaxSessionMs while human stays online -> must not disconnect
    fakeNowMs += 1_000;
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(server.runtime.connected, true);
    assert.equal(server.runtime._soloSinceMs, null);

    // Human chat also preserves cleared _soloSinceMs
    bot.emit("chat", "M1haiier", "hi hakimi");
    assert.equal(server.runtime._soloSinceMs, null);

    // Human leaves -> fresh solo clock starts from leave time, does not immediately disconnect
    bot.players = {
      HakimiBot: { username: "HakimiBot" },
    };
    bot.emit("playerLeft", { username: "M1haiier" });
    assert.equal(server.runtime._soloSinceMs, fakeNowMs);

    fakeNowMs += 100;
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(server.runtime.connected, true);

    ws.close();
  } finally {
    await server.close();
  }
});

test("solo_session_limit cancels in-flight slow goal cleanly and remains in 待机", async () => {
  const { factory, getLastBot } = createMockBotFactory({ slowGoto: true, slowGotoMs: 2000 });
  let fakeNowMs = 1_700_000_000_000;

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
    },
    createBotFn: factory,
    snapshotIntervalMs: 25,
    idleDisconnectMs: 0,
    soloMaxSessionMs: 150,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    const bot = getLastBot();

    const goalPromise = rpcCall(ws, inbox, "slow-solo-goal", "mc.dispatch_goal", {
      action_type: "goto",
      x: 50,
      y: 64,
      z: 50,
    });
    await inbox.next((m) => m.event === "mc.snapshot" && m.payload.active_task === "goto");

    fakeNowMs += 200;
    const soloMilestone = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_idle_disconnected",
      2000,
    );
    assert.equal(soloMilestone.payload.payload.reason, "solo_session_limit");

    const goalRes = await goalPromise;
    assert.equal(goalRes.ok, true);
    assert.equal(goalRes.data.ok, false);
    assert.equal(goalRes.data.reason, "mc_goal_cancelled");
    assert.equal(server.runtime.connected, false);
    assert.equal(server.runtime.activity, "待机");
    assert.ok(bot.stoppedCalls >= 1);

    ws.close();
  } finally {
    await server.close();
  }
});
