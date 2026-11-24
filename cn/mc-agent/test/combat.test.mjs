import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import vec3 from "vec3";
import { WebSocket } from "ws";
import {
  classifyCombatEntity,
  createCombatState,
  runGuardCombatStep,
} from "../src/combat.mjs";
import { createMcBridgeServer } from "../src/index.mjs";

const VALID_TOKEN = "g".repeat(40);

function createCombatMockBot({
  pos = vec3(0, 64, 0),
  health = 20,
  food = 20,
  items = [],
  heldItem = null,
  offHand = null,
  entities = {},
  players = {},
} = {}) {
  const bot = new EventEmitter();
  bot.username = "HakimiBot";
  bot.game = { dimension: "overworld" };
  bot.health = health;
  bot.food = food;
  bot.entity = { position: pos.clone() };
  bot.entities = entities;
  bot.players = players;
  bot.heldItem = heldItem;
  bot.attackCalls = [];
  bot.activateCalls = [];
  bot.deactivateCalls = 0;
  bot.movementsLog = [];
  bot.setGoals = [];
  bot.chatCalls = [];
  bot.consumeCalls = [];

  const slots = new Array(46).fill(null);
  if (offHand) slots[45] = offHand;
  items.forEach((it, idx) => {
    if (9 + idx < 45) slots[9 + idx] = it;
  });

  bot.inventory = {
    inventoryStart: 9,
    inventoryEnd: 45,
    hotbarStart: 36,
    slots,
    items: () => slots.slice(9, 45).filter(Boolean),
    emptySlotCount: () => slots.slice(9, 45).filter((x) => !x).length,
  };

  bot.equip = async (item, dest) => {
    if (dest === "hand") bot.heldItem = item;
    else if (dest === "off-hand") slots[45] = item;
  };
  bot.attack = (ent) => {
    bot.attackCalls.push(ent.name || ent.username || ent.type);
  };
  bot.activateItem = (offhand) => {
    bot.activateCalls.push(Boolean(offhand));
  };
  bot.deactivateItem = () => {
    bot.deactivateCalls += 1;
  };
  bot.consume = async () => {
    bot.consumeCalls.push(bot.heldItem?.name || "");
    bot.food = Math.min(20, (bot.food ?? 0) + 6);
  };
  bot.lookAt = async () => {};
  bot.swingArm = () => {};
  bot.chat = (m) => bot.chatCalls.push(m);
  bot.pathfinder = {
    setMovements: (m) => bot.movementsLog.push(m),
    setGoal: (g, dyn) => bot.setGoals.push({ goal: g, dynamic: dyn }),
    stop: () => {},
    goto: async () => {},
  };
  bot.quit = () => bot.emit("end", "quit");
  bot.end = () => bot.emit("end", "end");
  return bot;
}

test("classifyCombatEntity enforces zero PvP, neutral/pet protection, creeper standoff, and high-danger/enderman rules", () => {
  const bot = createCombatMockBot();
  assert.equal(
    classifyCombatEntity({ type: "player", username: "Enemy", name: "zombie", position: vec3(1, 64, 0) }, bot),
    "protected",
  );
  assert.equal(classifyCombatEntity({ name: "iron_golem", position: vec3(1, 64, 0) }, bot), "protected");
  assert.equal(classifyCombatEntity({ name: "villager", position: vec3(1, 64, 0) }, bot), "protected");
  assert.equal(classifyCombatEntity({ name: "wolf", tamed: true, owner: "Boiser", position: vec3(1, 64, 0) }, bot), "protected");
  assert.equal(classifyCombatEntity({ name: "cat", position: vec3(1, 64, 0) }, bot), "protected");
  assert.equal(classifyCombatEntity({ name: "zombie", tamed: true, position: vec3(1, 64, 0) }, bot), "protected");

  assert.equal(classifyCombatEntity({ name: "creeper", position: vec3(2, 64, 0) }, bot), "creeper");
  assert.equal(classifyCombatEntity({ name: "warden", position: vec3(2, 64, 0) }, bot), "avoid");
  assert.equal(classifyCombatEntity({ name: "ravager", position: vec3(2, 64, 0) }, bot), "avoid");
  assert.equal(classifyCombatEntity({ name: "piglin_brute", position: vec3(2, 64, 0) }, bot), "avoid");
  assert.equal(classifyCombatEntity({ name: "enderman", position: vec3(2, 64, 0) }, bot), "avoid");
  assert.equal(
    classifyCombatEntity({ name: "enderman", isScreaming: true, position: vec3(2, 64, 0) }, bot),
    "engageable",
  );
  assert.equal(classifyCombatEntity({ name: "zombie", position: vec3(2, 64, 0) }, bot), "engageable");
  assert.equal(classifyCombatEntity({ name: "skeleton", position: vec3(2, 64, 0) }, bot), "engageable");
});

test("guard combat NEVER attacks players, iron_golems, villagers, tamed pets, or unprovoked endermen", async () => {
  const bot = createCombatMockBot({
    items: [{ name: "iron_sword", count: 1 }],
    entities: {
      p1: { type: "player", username: "Stranger", position: vec3(1.5, 64, 0) },
      ig: { name: "iron_golem", position: vec3(1.5, 64, 0) },
      v1: { name: "villager", position: vec3(1.5, 64, 0) },
      w1: { name: "wolf", tamed: true, owner: "Boiser", position: vec3(1.5, 64, 0) },
      e1: { name: "enderman", position: vec3(2, 64, 0) },
    },
  });
  const state = createCombatState();
  const res = await runGuardCombatStep({
    bot,
    combatState: state,
    health: 20,
    food: 20,
    nowMs: 10_000,
  });
  assert.equal(res.handled, false);
  assert.equal(bot.attackCalls.length, 0);
});

test("guard combat enforces leash radii (<=10 guard, <=7 self-defense, <=14 max leash) and canDig=false", async () => {
  const player = { username: "Boiser", entity: { position: vec3(0, 64, 0) } };

  // 1. Solo self-defense: 8 blocks ignored, 6.5 blocks engaged with canDig=false
  const soloFar = createCombatMockBot({
    pos: vec3(0, 64, 0),
    entities: { z1: { id: 1, name: "zombie", position: vec3(8, 64, 0) } },
  });
  const r1 = await runGuardCombatStep({
    bot: soloFar,
    combatState: createCombatState(),
    guardedPlayer: null,
    health: 20,
    food: 20,
    nowMs: 10_000,
  });
  assert.equal(r1.handled, false);

  const soloNear = createCombatMockBot({
    pos: vec3(0, 64, 0),
    entities: { z1: { id: 1, name: "zombie", position: vec3(6.5, 64, 0) } },
  });
  const r2 = await runGuardCombatStep({
    bot: soloNear,
    combatState: createCombatState(),
    guardedPlayer: null,
    health: 20,
    food: 20,
    nowMs: 10_000,
  });
  assert.equal(r2.action, "approach_hostile");
  assert.ok(soloNear.movementsLog.length >= 1);
  assert.ok(soloNear.movementsLog.every((m) => m.canDig === false));

  // 2. Guard player: zombie at 9.5 blocks from player (9.5 from bot too -> >7 self-defense but <=10 guard!) -> engages!
  const guardHit = createCombatMockBot({
    pos: vec3(0, 64, 0),
    entities: { z1: { id: 1, name: "zombie", position: vec3(9.5, 64, 0) } },
  });
  const r3 = await runGuardCombatStep({
    bot: guardHit,
    combatState: createCombatState(),
    guardedPlayer: player,
    health: 20,
    food: 20,
    nowMs: 10_000,
  });
  assert.equal(r3.action, "approach_hostile");
  assert.ok(guardHit.movementsLog.every((m) => m.canDig === false));

  // 3. Mob > 14 blocks from guarded player (even if within 7 of bot) -> NEVER chased; if bot > 14 from player -> returns to player!
  const leashBot = createCombatMockBot({
    pos: vec3(15, 64, 0),
    entities: { z1: { id: 1, name: "zombie", position: vec3(16, 64, 0) } },
  });
  const r4 = await runGuardCombatStep({
    bot: leashBot,
    combatState: createCombatState(),
    guardedPlayer: player,
    health: 20,
    food: 20,
    nowMs: 10_000,
  });
  assert.equal(r4.action, "leash_return");
  assert.equal(leashBot.attackCalls.length, 0);
  assert.ok(leashBot.movementsLog.every((m) => m.canDig === false));
});

test("guard combat enforces 1.9+ attack speed (625ms sword / 1000ms axe) and raises shield against skeletons", async () => {
  // Sword 625ms cadence
  const swordBot = createCombatMockBot({
    items: [{ name: "iron_sword", count: 1 }, { name: "wooden_sword", count: 1 }],
    entities: { z1: { id: 1, name: "zombie", position: vec3(2, 64, 0) } },
  });
  const stSword = createCombatState();
  const s1 = await runGuardCombatStep({ bot: swordBot, combatState: stSword, health: 20, food: 20, nowMs: 1000 });
  assert.equal(s1.attacked, true);
  assert.equal(swordBot.heldItem?.name, "iron_sword");

  const s2 = await runGuardCombatStep({ bot: swordBot, combatState: stSword, health: 20, food: 20, nowMs: 1500 });
  assert.equal(s2.attacked, false);
  assert.equal(swordBot.attackCalls.length, 1);

  const s3 = await runGuardCombatStep({ bot: swordBot, combatState: stSword, health: 20, food: 20, nowMs: 1625 });
  assert.equal(s3.attacked, true);
  assert.equal(swordBot.attackCalls.length, 2);

  // Axe 1000ms cadence
  const axeBot = createCombatMockBot({
    items: [{ name: "diamond_axe", count: 1 }],
    entities: { z1: { id: 1, name: "zombie", position: vec3(2, 64, 0) } },
  });
  const stAxe = createCombatState();
  await runGuardCombatStep({ bot: axeBot, combatState: stAxe, health: 20, food: 20, nowMs: 2000 });
  assert.equal(axeBot.attackCalls.length, 1);
  await runGuardCombatStep({ bot: axeBot, combatState: stAxe, health: 20, food: 20, nowMs: 2800 });
  assert.equal(axeBot.attackCalls.length, 1);
  await runGuardCombatStep({ bot: axeBot, combatState: stAxe, health: 20, food: 20, nowMs: 3000 });
  assert.equal(axeBot.attackCalls.length, 2);

  // Skeleton with off-hand shield: raises shield while approaching, lowers before melee attack
  const skBot = createCombatMockBot({
    items: [{ name: "iron_sword", count: 1 }],
    offHand: { name: "shield", count: 1 },
    entities: { sk1: { id: 2, name: "skeleton", position: vec3(6, 64, 0) } },
  });
  const stSk = createCombatState();
  const ap = await runGuardCombatStep({ bot: skBot, combatState: stSk, health: 20, food: 20, nowMs: 5000 });
  assert.equal(ap.action, "approach_hostile");
  assert.equal(stSk.shieldRaised, true);
  assert.deepEqual(skBot.activateCalls, [true]);

  // Move skeleton into melee reach -> lowers shield and attacks!
  skBot.entities.sk1.position = vec3(2, 64, 0);
  const hit = await runGuardCombatStep({ bot: skBot, combatState: stSk, health: 20, food: 20, nowMs: 5700 });
  assert.equal(hit.attacked, true);
  assert.equal(stSk.shieldRaised, false);
  assert.equal(skBot.deactivateCalls, 1);
  assert.deepEqual(skBot.attackCalls, ["skeleton"]);
});

test("creeper standoff (>6 blocks, NEVER melee) and high-danger mob avoidance (warden/ravager/piglin_brute)", async () => {
  const creeperAlerts = [];
  const bot = createCombatMockBot({
    items: [{ name: "netherite_sword", count: 1 }],
    entities: {
      c1: { id: 10, name: "creeper", position: vec3(4, 64, 0) },
      z1: { id: 11, name: "zombie", position: vec3(2, 64, 0) },
    },
  });
  const st = createCombatState();
  const res = await runGuardCombatStep({
    bot,
    combatState: st,
    health: 20,
    food: 20,
    nowMs: 1000,
    onCreeperAlert: (c) => creeperAlerts.push(c),
  });
  assert.equal(res.action, "creeper_standoff");
  assert.equal(bot.attackCalls.length, 0);
  assert.equal(creeperAlerts.length, 1);
  assert.ok(bot.movementsLog.every((m) => m.canDig === false));
  // Retreat goal should point away from creeper (negative X direction > 6 blocks away)
  assert.ok(bot.setGoals[0].goal.x < -5);

  for (const dangerous of ["warden", "ravager", "piglin_brute"]) {
    const dBot = createCombatMockBot({
      items: [{ name: "netherite_sword", count: 1 }],
      entities: { d1: { id: 20, name: dangerous, position: vec3(5, 64, 0) } },
    });
    const dRes = await runGuardCombatStep({
      bot: dBot,
      combatState: createCombatState(),
      health: 20,
      food: 20,
      nowMs: 2000,
    });
    assert.equal(dRes.action, "avoid_danger");
    assert.equal(dBot.attackCalls.length, 0);
    assert.ok(dBot.movementsLog.every((m) => m.canDig === false));
  }
});

test("low-health retreat & heal hysteresis: health <= 7 retreats & triggers eat, stays retreating at 10, resumes at >= 12", async () => {
  let healEatCalls = 0;
  let retreatAlerts = 0;
  const bot = createCombatMockBot({
    items: [{ name: "iron_sword", count: 1 }],
    entities: { z1: { id: 1, name: "zombie", position: vec3(2, 64, 0) } },
  });
  const st = createCombatState();

  // HP = 7 -> retreat + heal eat, NO attack
  const r1 = await runGuardCombatStep({
    bot,
    combatState: st,
    health: 7,
    food: 14,
    nowMs: 1000,
    onNeedHealEat: () => {
      healEatCalls += 1;
    },
    onRetreatAlert: () => {
      retreatAlerts += 1;
    },
  });
  assert.equal(r1.action, "retreat_heal");
  assert.equal(st.retreating, true);
  assert.equal(bot.attackCalls.length, 0);
  assert.equal(healEatCalls, 1);
  assert.equal(retreatAlerts, 1);
  assert.ok(bot.movementsLog.every((m) => m.canDig === false));

  // HP = 10 -> hysteresis keeps retreating!
  const r2 = await runGuardCombatStep({
    bot,
    combatState: st,
    health: 10,
    food: 16,
    nowMs: 2000,
    onNeedHealEat: () => {
      healEatCalls += 1;
    },
  });
  assert.equal(r2.action, "retreat_heal");
  assert.equal(st.retreating, true);
  assert.equal(bot.attackCalls.length, 0);
  assert.equal(healEatCalls, 2);

  // HP = 12 -> exits retreat and resumes fighting!
  const r3 = await runGuardCombatStep({
    bot,
    combatState: st,
    health: 12,
    food: 18,
    nowMs: 3000,
  });
  assert.equal(st.retreating, false);
  assert.equal(r3.attacked, true);
  assert.deepEqual(bot.attackCalls, ["zombie"]);
});

test("WS integration: guard/evade RPC & whitelisted chat toggle mode, enforce allowlist, and emit mc_creeper_alert", async () => {
  let lastBot = null;
  let fakeNowMs = 1_700_000_000_000;

  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
      MC_ALLOW_CHAT: "true",
      MC_ALLOWED_PLAYERS: "Boiser",
      MC_COMPANION_MODE: "true",
      MC_COMBAT_MODE: "evade",
    },
    createBotFn: () => {
      const bot = createCombatMockBot({
        pos: vec3(0, 64, 0),
        items: [{ name: "iron_sword", count: 1 }],
        players: {
          Boiser: { username: "Boiser", entity: { position: vec3(2, 64, 0) } },
        },
      });
      lastBot = bot;
      queueMicrotask(() => bot.emit("spawn"));
      return bot;
    },
    snapshotIntervalMs: 5000,
    companionTickMs: 30,
    survivalTickMs: 5000,
    milestoneCooldownMs: 0,
    chatCooldownMs: 0,
    companionReplyCooldownMs: 0,
    hostileAlertCooldownMs: 0,
    nowFn: () => fakeNowMs,
  });

  const addr = await server.start();
  const ws = new WebSocket(`ws://127.0.0.1:${addr.port}/ws`, {
    headers: { Authorization: `Bearer ${VALID_TOKEN}` },
  });
  const queue = [];
  const waiters = [];
  ws.on("message", (raw) => {
    const msg = JSON.parse(raw.toString("utf8"));
    const idx = waiters.findIndex((w) => !w.pred || w.pred(msg));
    if (idx !== -1) {
      const w = waiters.splice(idx, 1)[0];
      clearTimeout(w.t);
      w.res(msg);
      return;
    }
    queue.push(msg);
  });
  const next = (pred) => {
    const idx = queue.findIndex((m) => !pred || pred(m));
    if (idx !== -1) return Promise.resolve(queue.splice(idx, 1)[0]);
    return new Promise((res, rej) => {
      const w = { pred, res, t: setTimeout(() => rej(new Error("timeout")), 3000) };
      waiters.push(w);
    });
  };
  const rpc = (id, action, params) => {
    ws.send(JSON.stringify({ type: "rpc_request", id, action, params }));
    return next((m) => m.type === "rpc_response" && m.id === id);
  };
  await new Promise((r) => ws.once("open", r));

  try {
    await next((m) => m.event === "mc.hello");
    await next((m) => m.event === "mc.snapshot" && m.payload.connected === true);

    // Reject non-whitelisted guard target
    const denied = await rpc("g-deny", "mc.dispatch_goal", {
      action_type: "guard",
      target_player: "Stranger",
    });
    assert.equal(denied.data.ok, false);
    assert.equal(denied.data.reason, "player_not_allowed");

    // Switch to guard via RPC
    const guardRes = await rpc("g-ok", "mc.dispatch_goal", {
      action_type: "guard",
      target_player: "Boiser",
    });
    assert.equal(guardRes.data.ok, true);
    assert.equal(guardRes.data.combat_mode, "guard");
    assert.equal(guardRes.data.guard_target, "Boiser");
    assert.equal(guardRes.data.snapshot.combat_mode, "guard");
    assert.equal(guardRes.data.snapshot.guard_target, "Boiser");

    // Spawn creeper at 4 blocks -> emits mc_creeper_alert and NEVER attacks creeper
    lastBot.entities = { c1: { id: 99, name: "creeper", position: vec3(4, 64, 0) } };
    const creeperEvt = await next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_creeper_alert",
    );
    assert.equal(creeperEvt.payload.importance, "high");
    assert.equal(creeperEvt.payload.payload.action, "retreat_standoff");
    assert.equal(creeperEvt.payload.payload.combat_mode, "guard");
    assert.equal(lastBot.attackCalls.length, 0);

    // Clear creeper and switch to evade via RPC
    lastBot.entities = {};
    const evadeRes = await rpc("e-ok", "mc.dispatch_goal", { action_type: "evade" });
    assert.equal(evadeRes.data.ok, true);
    assert.equal(evadeRes.data.combat_mode, "evade");
    assert.equal(evadeRes.data.guard_target, "");
    assert.equal(evadeRes.data.snapshot.combat_mode, "evade");
    assert.equal(evadeRes.data.snapshot.guard_target, "");

    // Whitelisted chat toggles guard / evade; stranger ignored
    lastBot.emit("chat", "Stranger", "护卫");
    assert.equal(server.runtime.combatMode, "evade");

    lastBot.emit("chat", "Boiser", "保护我");
    assert.equal(server.runtime.combatMode, "guard");
    assert.equal(server.runtime.guardTarget, "Boiser");

    // Negation command "别打怪" must switch to evade (NOT match "打怪" as guard!)
    lastBot.emit("chat", "Boiser", "别打怪");
    assert.equal(server.runtime.combatMode, "evade");
    assert.equal(server.runtime.guardTarget, "");

    // In evade mode: zombie at 3 blocks (-X) + creeper at 5 blocks (+X) must prioritize creeper standoff
    // (retreat in -X away from creeper) and still emit mc_creeper_alert
    lastBot.setGoals.length = 0;
    lastBot.entities = {
      z1: { id: 101, name: "zombie", position: vec3(-3, 64, 0) },
      c2: { id: 102, name: "creeper", position: vec3(5, 64, 0) },
    };
    const evadeCreeperEvt = await next(
      (m) =>
        m.event === "mc.milestone" &&
        m.payload.event_type === "mc_creeper_alert" &&
        m.payload.payload.combat_mode === "evade",
    );
    assert.equal(evadeCreeperEvt.payload.importance, "high");
    assert.equal(evadeCreeperEvt.payload.payload.action, "retreat_standoff");
    assert.equal(server.runtime.activity, "远离苦力怕");
    assert.ok(lastBot.setGoals.some((g) => g.goal && g.goal.x < -5));

    ws.close();
  } finally {
    await server.close();
  }
});
