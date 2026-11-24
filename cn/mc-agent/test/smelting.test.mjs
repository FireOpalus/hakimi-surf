import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import vec3 from "vec3";
import { WebSocket } from "ws";
import { createMcBridgeServer } from "../src/index.mjs";
import { executeSmeltAction, parseSmeltParams, pickBestFuel } from "../src/smelting.mjs";

const VALID_TOKEN = "s".repeat(40);

const ITEMS_TABLE = {
  1: { id: 1, name: "oak_log" },
  2: { id: 2, name: "oak_planks" },
  3: { id: 3, name: "stick" },
  4: { id: 4, name: "coal" },
  5: { id: 5, name: "cobblestone" },
  6: { id: 6, name: "crafting_table" },
  7: { id: 7, name: "furnace" },
  8: { id: 8, name: "raw_iron" },
  9: { id: 9, name: "iron_ingot" },
  10: { id: 10, name: "beef" },
  11: { id: 11, name: "cooked_beef" },
  12: { id: 12, name: "charcoal" },
};

const ITEMS_BY_NAME = Object.fromEntries(
  Object.values(ITEMS_TABLE).map((it) => [it.name, it]),
);

function makeRecipe(resultName, resultCount, ingredientsObj, requiresTable = false) {
  const delta = [];
  for (const [ingName, qty] of Object.entries(ingredientsObj)) {
    delta.push({ id: ITEMS_BY_NAME[ingName].id, count: -qty });
  }
  delta.push({ id: ITEMS_BY_NAME[resultName].id, count: resultCount });
  return {
    result: { id: ITEMS_BY_NAME[resultName].id, count: resultCount },
    delta,
    requiresTable,
  };
}

const ALL_RECIPES = {
  oak_planks: [makeRecipe("oak_planks", 4, { oak_log: 1 }, false)],
  crafting_table: [makeRecipe("crafting_table", 1, { oak_planks: 4 }, false)],
  furnace: [makeRecipe("furnace", 1, { cobblestone: 8 }, true)],
};

function createSmeltMockBot({
  items = {},
  blocks = new Map(),
  pos = vec3(10, 64, 10),
  timeOfDay = 4000,
  smeltTicksPerItem = 1,
} = {}) {
  const bot = new EventEmitter();
  bot.username = "HakimiBot";
  bot.game = { dimension: "overworld" };
  bot.time = { timeOfDay };
  bot.health = 20;
  bot.food = 20;
  bot.entity = { position: pos.clone() };
  bot.entities = {};
  bot.players = {};
  bot.heldItem = null;
  bot.registry = {
    items: ITEMS_TABLE,
    itemsByName: ITEMS_BY_NAME,
    blocksByName: {
      crafting_table: { id: 58, name: "crafting_table" },
      furnace: { id: 61, name: "furnace" },
    },
  };
  bot.gotoCalls = [];
  bot.digCalls = [];
  bot.placeCalls = [];
  bot.openFurnaceCalls = [];

  const slots = new Array(46).fill(null);
  let cursor = 9;
  for (const [name, count] of Object.entries(items)) {
    if (count > 0) {
      const id = ITEMS_BY_NAME[name]?.id ?? 999;
      slots[cursor] = { id, type: id, name, count };
      cursor += 1;
    }
  }

  function getCount(name) {
    let total = 0;
    for (let i = 9; i < 45; i += 1) {
      if (slots[i]?.name === name) total += slots[i].count;
    }
    return total;
  }

  function consumeItem(name, qty) {
    let rem = qty;
    for (let i = 9; i < 45 && rem > 0; i += 1) {
      if (slots[i]?.name === name) {
        const take = Math.min(slots[i].count, rem);
        slots[i].count -= take;
        rem -= take;
        if (slots[i].count <= 0) slots[i] = null;
      }
    }
    if (rem > 0) throw new Error(`missing_${name}`);
  }

  function addItem(name, qty) {
    const id = ITEMS_BY_NAME[name]?.id ?? 999;
    for (let i = 9; i < 45; i += 1) {
      if (slots[i]?.name === name) {
        slots[i].count += qty;
        return;
      }
    }
    for (let i = 9; i < 45; i += 1) {
      if (!slots[i]) {
        slots[i] = { id, type: id, name, count: qty };
        return;
      }
    }
  }

  bot.inventory = {
    slots,
    inventoryStart: 9,
    inventoryEnd: 45,
    items: () => slots.slice(9, 45).filter(Boolean),
    emptySlotCount: () => slots.slice(9, 45).filter((s) => !s).length,
  };

  bot.equip = async (item, dest = "hand") => {
    if (dest === "hand") bot.heldItem = item;
  };

  bot.recipesAll = (itemId, _meta, table) => {
    const name = ITEMS_TABLE[itemId]?.name;
    const list = ALL_RECIPES[name] || [];
    if (!table) return list.filter((r) => !r.requiresTable);
    return list;
  };

  bot.recipesFor = (itemId, _meta, minResultCount, table) => {
    const candidates = bot.recipesAll(itemId, null, table);
    return candidates.filter((r) => {
      for (const d of r.delta) {
        if (d.count < 0) {
          const ingName = ITEMS_TABLE[d.id]?.name;
          if (getCount(ingName) < -d.count * (minResultCount || 1)) return false;
        }
      }
      return true;
    });
  };

  bot.craft = async (recipe, count = 1) => {
    for (let i = 0; i < count; i += 1) {
      for (const d of recipe.delta) {
        if (d.count < 0) consumeItem(ITEMS_TABLE[d.id].name, -d.count);
      }
      addItem(ITEMS_TABLE[recipe.result.id].name, recipe.result.count);
    }
  };

  const keyOf = (p) => `${Math.floor(p.x)},${Math.floor(p.y)},${Math.floor(p.z)}`;
  bot.blockAt = (p) => {
    const k = keyOf(p);
    if (blocks.has(k)) return blocks.get(k);
    if (Math.floor(p.y) < 64) {
      return { name: "stone", position: vec3(Math.floor(p.x), Math.floor(p.y), Math.floor(p.z)), boundingBox: "block" };
    }
    return { name: "air", position: vec3(Math.floor(p.x), Math.floor(p.y), Math.floor(p.z)), boundingBox: "empty" };
  };

  bot.findBlocks = ({ matching, point, maxDistance = 16, count = 16 }) => {
    const origin = point ?? bot.entity.position;
    const found = [];
    for (const blk of blocks.values()) {
      if (!matching(blk)) continue;
      const dist = origin.distanceTo(blk.position);
      if (dist <= maxDistance) found.push({ pos: blk.position, dist });
    }
    found.sort((a, b) => a.dist - b.dist);
    return found.slice(0, count).map((f) => f.pos);
  };

  bot.placeBlock = async (refBlock, faceVec) => {
    const placedPos = vec3(
      Math.floor(refBlock.position.x + faceVec.x),
      Math.floor(refBlock.position.y + faceVec.y),
      Math.floor(refBlock.position.z + faceVec.z),
    );
    const heldName = bot.heldItem?.name || "furnace";
    consumeItem(heldName, 1);
    const blk = { name: heldName, position: placedPos, boundingBox: "block" };
    blocks.set(keyOf(placedPos), blk);
    bot.placeCalls.push({ name: heldName, pos: [placedPos.x, placedPos.y, placedPos.z] });
  };

  bot.pathfinder = {
    setMovements() {},
    stop() {},
    setGoal() {},
    async goto(goal) {
      bot.gotoCalls.push(goal);
      if (goal && typeof goal.x === "number" && typeof goal.z === "number") {
        const range = typeof goal.range === "number" ? goal.range : 0;
        const nextY = typeof goal.y === "number" ? goal.y : bot.entity.position.y;
        // Simulate stopping at `range` blocks away along X so pickup walk is genuinely exercised!
        const dx = goal.x - bot.entity.position.x;
        const stopX = Math.abs(dx) > range ? goal.x - Math.sign(dx) * range : goal.x;
        bot.entity.position = vec3(stopX, nextY, goal.z);
        // Simulate picking up any dropped item entity within 1.5 blocks
        for (const [eid, ent] of Object.entries(bot.entities)) {
          if (ent?.name === "item" && ent.position && bot.entity.position.distanceTo(ent.position) <= 1.5) {
            if (ent._dropItem) addItem(ent._dropItem, ent._dropCount || 1);
            delete bot.entities[eid];
          }
        }
      }
    },
  };

  bot.dig = async (block) => {
    bot.digCalls.push({ name: block.name, pos: [block.position.x, block.position.y, block.position.z] });
    blocks.delete(keyOf(block.position));
    // Spawn dropped item entity at the dug block's position (3 blocks away from bot!)
    const dropId = `drop-${bot.digCalls.length}`;
    bot.entities[dropId] = {
      id: dropId,
      name: "item",
      type: "object",
      position: block.position.clone(),
      _dropItem: block.name === "stone" ? "cobblestone" : block.name,
      _dropCount: 1,
    };
  };

  bot.openFurnace = async (furnaceBlock) => {
    bot.openFurnaceCalls.push(furnaceBlock.name);
    let inSlot = null;
    let fuelSlot = null;
    let outSlot = null;
    let polls = 0;
    const outputMap = { raw_iron: "iron_ingot", beef: "cooked_beef", oak_log: "charcoal" };

    return {
      inputItem: () => {
        if (inSlot && inSlot.count > 0 && fuelSlot) {
          polls += 1;
          if (polls >= smeltTicksPerItem) {
            polls = 0;
            inSlot.count -= 1;
            const outName = outputMap[inSlot.name] || "iron_ingot";
            if (!outSlot) outSlot = { name: outName, count: 1 };
            else outSlot.count += 1;
            if (inSlot.count <= 0) inSlot = null;
          }
        }
        return inSlot;
      },
      outputItem: () => outSlot,
      putFuel: async (typeId, _meta, count) => {
        const name = ITEMS_TABLE[typeId]?.name || "coal";
        consumeItem(name, count);
        fuelSlot = { name, count };
      },
      putInput: async (typeId, _meta, count) => {
        const name = ITEMS_TABLE[typeId]?.name || "raw_iron";
        consumeItem(name, count);
        inSlot = { name, count };
      },
      takeOutput: async () => {
        if (!outSlot) return null;
        const taken = { ...outSlot };
        addItem(taken.name, taken.count);
        outSlot = null;
        return taken;
      },
      close: () => {},
    };
  };

  bot._getCount = getCount;
  return bot;
}

function connectWs(url, headers) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, { headers });
    const queue = [];
    const waiters = [];
    ws.on("message", (raw) => {
      const msg = JSON.parse(raw.toString("utf8"));
      for (let i = 0; i < waiters.length; i += 1) {
        if (waiters[i].pred(msg)) {
          const [w] = waiters.splice(i, 1);
          w.resolve(msg);
          return;
        }
      }
      queue.push(msg);
    });
    const inbox = {
      queue,
      next(pred, timeoutMs = 2500) {
        for (let i = 0; i < queue.length; i += 1) {
          if (pred(queue[i])) return Promise.resolve(queue.splice(i, 1)[0]);
        }
        return new Promise((res, rej) => {
          const t = setTimeout(() => rej(new Error("ws_inbox_timeout")), timeoutMs);
          waiters.push({
            pred,
            resolve: (m) => {
              clearTimeout(t);
              res(m);
            },
          });
        });
      },
    };
    ws.once("open", () => resolve({ ws, inbox }));
    ws.once("error", reject);
  });
}

async function rpcCall(ws, inbox, id, action, params = {}) {
  ws.send(JSON.stringify({ type: "rpc_request", id, action, params }));
  return await inbox.next((m) => m.type === "rpc_response" && m.id === id);
}

test("parseSmeltParams and pickBestFuel validate canonical IDs and avoid starving input logs", () => {
  assert.deepEqual(parseSmeltParams({ item_name: "minecraft:raw_iron", count: 3 }), {
    itemName: "raw_iron",
    count: 3,
    fuelName: "",
  });
  assert.equal(parseSmeltParams({ item_name: "raw_iron", count: 0 }).error, "invalid_smelt_count");
  assert.equal(parseSmeltParams({ item_name: "BAD ITEM" }).error, "invalid_item_name");
  assert.equal(parseSmeltParams({ item_name: "raw_iron", fuel_name: "bad fuel!" }).error, "invalid_fuel_name");

  const counts = new Map([
    ["oak_log", 2],
    ["stick", 4],
    ["coal", 3],
  ]);
  assert.equal(pickBestFuel(counts, { inputItemName: "raw_iron", targetCount: 2 }).fuelName, "coal");

  // Smelting all 2 oak_log into charcoal must not steal those 2 oak_log as fuel
  const logOnly = new Map([["oak_log", 2]]);
  assert.equal(pickBestFuel(logOnly, { inputItemName: "oak_log", targetCount: 2 }), null);
  assert.equal(
    pickBestFuel(new Map([["oak_log", 3]]), { inputItemName: "oak_log", targetCount: 2 }).fuelName,
    "oak_log",
  );
});

test("executeSmeltAction crafts/places furnace when missing and smelts raw_iron into iron_ingot", async () => {
  const bot = createSmeltMockBot({
    items: {
      raw_iron: 2,
      coal: 2,
      cobblestone: 8,
      oak_planks: 4,
    },
  });

  const res = await executeSmeltAction(
    bot,
    { item_name: "raw_iron", count: 2 },
    { pollIntervalMs: 10, maxSmeltWaitMs: 1500 },
  );
  assert.equal(res.ok, true);
  assert.equal(res.output_item, "iron_ingot");
  assert.equal(res.smelted_count, 2);
  assert.equal(res.used_fuel, "coal");
  assert.equal(bot._getCount("iron_ingot"), 2);
  assert.ok(bot.placeCalls.some((c) => c.name === "furnace"));
  assert.deepEqual(bot.openFurnaceCalls, ["furnace"]);

  // Missing cobblestone -> returns structured no_furnace with missing_materials
  const bareBot = createSmeltMockBot({
    items: { raw_iron: 2, coal: 1, cobblestone: 3 },
  });
  const noFurnace = await executeSmeltAction(
    bareBot,
    { item_name: "raw_iron", count: 1 },
    { pollIntervalMs: 10, maxSmeltWaitMs: 500 },
  );
  assert.equal(noFurnace.ok, false);
  assert.equal(noFurnace.reason, "no_furnace");
  assert.equal(noFurnace.missing_materials[0].item, "cobblestone");
  assert.equal(noFurnace.missing_materials[0].shortfall, 5);
});

test("WS integration: snapshot exposes time_of_day & nearby_resources, gather collects drops x3, and mc.smelt succeeds", async () => {
  const blocks = new Map([
    ["14,64,10", { name: "oak_log", position: vec3(14, 64, 10), boundingBox: "block" }],
    ["15,64,10", { name: "oak_log", position: vec3(15, 64, 10), boundingBox: "block" }],
    ["16,64,10", { name: "oak_log", position: vec3(16, 64, 10), boundingBox: "block" }],
    ["12,64,12", { name: "furnace", position: vec3(12, 64, 12), boundingBox: "block" }],
    ["11,64,12", { name: "iron_ore", position: vec3(11, 64, 12), boundingBox: "block" }],
  ]);

  let lastBot = null;
  const server = createMcBridgeServer({
    env: {
      MC_BRIDGE_HOST: "127.0.0.1",
      MC_BRIDGE_PORT: "0",
      MC_BRIDGE_TOKEN: VALID_TOKEN,
      MC_ENABLED: "true",
      MC_HOST: "mock.mc.internal",
      MC_AUTO_CONNECT: "true",
    },
    createBotFn: () => {
      const bot = createSmeltMockBot({
        items: { raw_iron: 2, coal: 1 },
        blocks,
        pos: vec3(10, 64, 10),
        timeOfDay: 15000, // night
      });
      lastBot = bot;
      queueMicrotask(() => bot.emit("spawn"));
      return bot;
    },
    snapshotIntervalMs: 5000,
    pickupSettleMs: 0,
    smeltPollIntervalMs: 10,
  });

  const addr = await server.start();
  const { ws, inbox } = await connectWs(`ws://127.0.0.1:${addr.port}/ws`, {
    Authorization: `Bearer ${VALID_TOKEN}`,
  });

  try {
    await inbox.next((m) => m.event === "mc.hello");
    const spawnSnap = await inbox.next((m) => m.event === "mc.snapshot" && m.payload.connected === true);
    assert.equal(spawnSnap.payload.time_of_day, "night");
    assert.ok(spawnSnap.payload.nearby_resources.includes("oak_log"));
    assert.ok(spawnSnap.payload.nearby_resources.includes("furnace"));
    assert.ok(spawnSnap.payload.nearby_resources.includes("iron_ore"));

    // Multi-block gather x3: walks in to pick up all 3 dropped oak_log items into inventory!
    const gatherRes = await rpcCall(ws, inbox, "gather-3", "mc.dispatch_goal", {
      action_type: "gather",
      block_type: "oak_log",
      count: 3,
    });
    assert.equal(gatherRes.ok, true);
    assert.equal(gatherRes.data.ok, true);
    assert.equal(gatherRes.data.collected_count, 3);
    assert.equal(lastBot._getCount("oak_log"), 3);
    assert.ok(gatherRes.data.snapshot.inventory_summary.includes("oak_log:3"));

    // mc.smelt uses nearby furnace to smelt 2 raw_iron -> 2 iron_ingot
    const smeltRes = await rpcCall(ws, inbox, "smelt-iron", "mc.dispatch_goal", {
      action_type: "smelt",
      item_name: "raw_iron",
      count: 2,
    });
    assert.equal(smeltRes.ok, true);
    assert.equal(smeltRes.data.ok, true);
    assert.equal(smeltRes.data.output_item, "iron_ingot");
    assert.equal(smeltRes.data.smelted_count, 2);
    assert.ok(smeltRes.data.snapshot.inventory_summary.includes("iron_ingot:2"));

    const smeltMilestone = await inbox.next(
      (m) => m.event === "mc.milestone" && m.payload.event_type === "mc_goal_completed" && m.payload.payload.action_type === "smelt",
    );
    assert.equal(smeltMilestone.payload.payload.output_item, "iron_ingot");
    assert.equal(smeltMilestone.payload.payload.smelted_count, 2);

    ws.close();
  } finally {
    await server.close();
  }
});
