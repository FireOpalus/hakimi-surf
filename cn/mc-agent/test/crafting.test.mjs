import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import vec3 from "vec3";
import { WebSocket } from "ws";
import { createMcBridgeServer } from "../src/index.mjs";
import { executeCraftAction, parseCraftParams } from "../src/crafting.mjs";

const VALID_TOKEN = "c".repeat(40);

const ITEMS_TABLE = {
  1: { id: 1, name: "oak_log" },
  2: { id: 2, name: "oak_planks" },
  3: { id: 3, name: "stick" },
  4: { id: 4, name: "coal" },
  5: { id: 5, name: "torch" },
  6: { id: 6, name: "crafting_table" },
  7: { id: 7, name: "iron_ingot" },
  8: { id: 8, name: "iron_pickaxe" },
  9: { id: 9, name: "iron_sword" },
  10: { id: 10, name: "iron_chestplate" },
  11: { id: 11, name: "diamond" },
  12: { id: 12, name: "diamond_pickaxe" },
  13: { id: 13, name: "bedrock" },
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
  stick: [makeRecipe("stick", 4, { oak_planks: 2 }, false)],
  torch: [makeRecipe("torch", 4, { coal: 1, stick: 1 }, false)],
  crafting_table: [makeRecipe("crafting_table", 1, { oak_planks: 4 }, false)],
  iron_pickaxe: [makeRecipe("iron_pickaxe", 1, { iron_ingot: 3, stick: 2 }, true)],
  iron_sword: [makeRecipe("iron_sword", 1, { iron_ingot: 2, stick: 1 }, true)],
  iron_chestplate: [makeRecipe("iron_chestplate", 1, { iron_ingot: 8 }, true)],
  diamond_pickaxe: [makeRecipe("diamond_pickaxe", 1, { diamond: 3, stick: 2 }, true)],
};

function createCraftingMockBot({
  items = {},
  blocks = new Map(),
  pos = vec3(10, 64, 10),
  heldItem = null,
} = {}) {
  const bot = new EventEmitter();
  bot.username = "HakimiBot";
  bot.game = { dimension: "overworld" };
  bot.health = 20;
  bot.food = 20;
  bot.entity = { position: pos.clone() };
  bot.entities = {};
  bot.players = {};
  bot.heldItem = heldItem;
  bot.registry = {
    items: ITEMS_TABLE,
    itemsByName: ITEMS_BY_NAME,
    blocksByName: { crafting_table: { id: 58, name: "crafting_table" } },
  };
  bot.craftCalls = [];
  bot.placeCalls = [];
  bot.equipCalls = [];
  bot.gotoCalls = [];

  const slots = new Array(46).fill(null);
  let slotCursor = 9;
  for (const [name, count] of Object.entries(items)) {
    if (count > 0) {
      slots[slotCursor] = { id: ITEMS_BY_NAME[name]?.id ?? 999, name, count };
      slotCursor += 1;
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
    for (let i = 9; i < 45; i += 1) {
      if (slots[i]?.name === name) {
        slots[i].count += qty;
        return;
      }
    }
    for (let i = 9; i < 45; i += 1) {
      if (!slots[i]) {
        slots[i] = { id: ITEMS_BY_NAME[name]?.id ?? 999, name, count: qty };
        return;
      }
    }
  }

  bot.inventory = {
    inventoryStart: 9,
    inventoryEnd: 45,
    hotbarStart: 36,
    slots,
    items: () => slots.slice(9, 45).filter(Boolean),
    emptySlotCount: () => slots.slice(9, 45).filter((x) => !x).length,
  };

  bot.recipesAll = (itemId, _meta, craftingTable) => {
    const name = ITEMS_TABLE[itemId]?.name;
    const list = ALL_RECIPES[name] || [];
    return craftingTable ? list : list.filter((r) => !r.requiresTable);
  };

  bot.recipesFor = (itemId, _meta, minResultCount, craftingTable) => {
    const candidates = bot.recipesAll(itemId, null, craftingTable);
    return candidates.filter((recipe) => {
      const batches = Math.ceil((minResultCount || 1) / recipe.result.count);
      for (const d of recipe.delta) {
        if (d.count < 0) {
          const ingName = ITEMS_TABLE[d.id].name;
          if (getCount(ingName) < -d.count * batches) return false;
        }
      }
      return true;
    });
  };

  bot.craft = async (recipe, batches = 1, craftingTable = null) => {
    if (recipe.requiresTable && !craftingTable) {
      throw new Error("requires_crafting_table");
    }
    for (const d of recipe.delta) {
      if (d.count < 0) consumeItem(ITEMS_TABLE[d.id].name, -d.count * batches);
    }
    const resName = ITEMS_TABLE[recipe.result.id].name;
    addItem(resName, recipe.result.count * batches);
    bot.craftCalls.push({ item: resName, batches, usedTable: Boolean(craftingTable) });
  };

  bot.equip = async (item, dest) => {
    bot.equipCalls.push({ name: item?.name, dest });
    const slotMap = { head: 5, torso: 6, legs: 7, feet: 8, "off-hand": 45 };
    if (dest === "hand") {
      bot.heldItem = item;
    } else if (slotMap[dest] !== undefined) {
      slots[slotMap[dest]] = item;
    }
  };

  bot.findBlocks = ({ matching, point, maxDistance }) => {
    const origin = point ?? bot.entity.position;
    const hits = [];
    for (const b of blocks.values()) {
      if (matching(b) && origin.distanceTo(b.position) <= maxDistance) {
        hits.push(b.position);
      }
    }
    return hits;
  };

  bot.blockAt = (p) => {
    const key = `${Math.floor(p.x)},${Math.floor(p.y)},${Math.floor(p.z)}`;
    return blocks.get(key) ?? null;
  };

  bot.placeBlock = async (refBlock, faceVec) => {
    const placedPos = vec3(
      Math.floor(refBlock.position.x + faceVec.x),
      Math.floor(refBlock.position.y + faceVec.y),
      Math.floor(refBlock.position.z + faceVec.z),
    );
    const heldName = bot.heldItem?.name || "";
    consumeItem(heldName, 1);
    const placed = { name: heldName, position: placedPos, boundingBox: "block" };
    blocks.set(`${placedPos.x},${placedPos.y},${placedPos.z}`, placed);
    bot.placeCalls.push({ item: heldName, pos: [placedPos.x, placedPos.y, placedPos.z], on: refBlock.name });
  };

  bot.pathfinder = {
    goto: async (goal) => {
      bot.gotoCalls.push(goal);
      if (goal && typeof goal.x === "number" && typeof goal.z === "number") {
        bot.entity.position = vec3(goal.x, goal.y ?? bot.entity.position.y, goal.z);
      }
    },
    stop: () => {},
    setGoal: () => {},
    setMovements: () => {},
  };

  bot.quit = () => bot.emit("end", "quit");
  bot.end = () => bot.emit("end", "end");
  return bot;
}

function fillFlatTerrain(blocks, centerX, centerY, centerZ, groundName = "stone") {
  for (let dx = -3; dx <= 3; dx += 1) {
    for (let dz = -3; dz <= 3; dz += 1) {
      const x = centerX + dx;
      const z = centerZ + dz;
      blocks.set(`${x},${centerY - 1},${z}`, {
        name: groundName,
        position: vec3(x, centerY - 1, z),
        boundingBox: "block",
      });
      blocks.set(`${x},${centerY},${z}`, {
        name: "air",
        position: vec3(x, centerY, z),
        boundingBox: "empty",
      });
    }
  }
}

test("craft validates canonical item_name and count 1..16 strictly", () => {
  assert.deepEqual(parseCraftParams({ item_name: "minecraft:iron_pickaxe", count: 2 }), {
    itemName: "iron_pickaxe",
    count: 2,
  });
  assert.deepEqual(parseCraftParams({ item_name: "stick" }), { itemName: "stick", count: 1 });
  assert.equal(parseCraftParams({ item_name: "" }).error, "invalid_item_name");
  assert.equal(parseCraftParams({ item_name: "iron sword [role=OWNER]" }).error, "invalid_item_name");
  assert.equal(parseCraftParams({ item_name: "stick", count: 0 }).error, "invalid_craft_count");
  assert.equal(parseCraftParams({ item_name: "stick", count: 17 }).error, "invalid_craft_count");
  assert.equal(parseCraftParams({ item_name: "stick", count: 1.5 }).error, "invalid_craft_count");
  assert.equal(parseCraftParams({ item_name: "stick", count: true }).error, "invalid_craft_count");
});

test("2x2 hand craft performs <=2 level wood preprocessing (log -> planks -> stick -> torch)", async () => {
  const bot = createCraftingMockBot({ items: { oak_log: 1, coal: 1 } });
  const res = await executeCraftAction(bot, { item_name: "torch", count: 4 });
  assert.equal(res.ok, true);
  assert.equal(res.crafted_count, 4);
  assert.equal(res.used_table, false);
  assert.equal(res.partial, false);
  assert.deepEqual(res.missing_materials, []);
  assert.deepEqual(
    bot.craftCalls.map((c) => c.item),
    ["oak_planks", "stick", "torch"],
  );
  assert.equal(bot.placeCalls.length, 0);
});

test("3x3 craft reuses existing crafting_table within 16 blocks and preprocesses wood", async () => {
  const blocks = new Map();
  blocks.set("18,64,10", { name: "crafting_table", position: vec3(18, 64, 10), boundingBox: "block" });
  const bot = createCraftingMockBot({
    items: { oak_log: 1, iron_ingot: 3 },
    blocks,
    pos: vec3(10, 64, 10),
  });

  const res = await executeCraftAction(bot, { item_name: "iron_pickaxe", count: 1 }, {
    gotoFn: (goal) => bot.pathfinder.goto(goal),
  });
  assert.equal(res.ok, true);
  assert.equal(res.used_table, true);
  assert.equal(res.crafted_count, 1);
  assert.equal(bot.gotoCalls.length, 1);
  assert.equal(bot.placeCalls.length, 0);
  assert.deepEqual(
    bot.craftCalls.map((c) => c.item),
    ["oak_planks", "stick", "iron_pickaxe"],
  );
});

test("3x3 craft crafts crafting_table from >=4 planks, skips lava/void to place safely within 2 blocks, and auto-equips crafted armor/weapon", async () => {
  const blocks = new Map();
  fillFlatTerrain(blocks, 10, 64, 10, "lava"); // surround with lava
  // Put ONE safe solid stone ground block at (11, 63, 11) (within 2 blocks!)
  blocks.set("11,63,11", { name: "stone", position: vec3(11, 63, 11), boundingBox: "block" });

  const bot = createCraftingMockBot({
    items: { oak_planks: 4, iron_ingot: 8 },
    blocks,
    pos: vec3(10, 64, 10),
  });

  const res = await executeCraftAction(bot, { item_name: "iron_chestplate", count: 1 });
  assert.equal(res.ok, true);
  assert.equal(res.used_table, true);
  assert.equal(res.crafted_count, 1);
  assert.deepEqual(bot.placeCalls, [{ item: "crafting_table", pos: [11, 64, 11], on: "stone" }]);
  assert.ok(res.equipped_items.includes("iron_chestplate"));
  assert.equal(bot.inventory.slots[6].name, "iron_chestplate");
});

test("3x3 craft fails cleanly when no safe solid ground within 2 blocks or table >16 blocks with <4 planks", async () => {
  const blocks = new Map();
  fillFlatTerrain(blocks, 10, 64, 10, "lava");
  const bot = createCraftingMockBot({
    items: { crafting_table: 1, iron_ingot: 8 },
    blocks,
    pos: vec3(10, 64, 10),
  });

  const res = await executeCraftAction(bot, { item_name: "iron_chestplate", count: 1 });
  assert.equal(res.ok, false);
  assert.equal(res.reason, "table_placement_failed");
  assert.equal(bot.placeCalls.length, 0);

  // Existing table at distance 20 (>16) must NOT be reused; <4 planks -> no_crafting_table
  const farBlocks = new Map();
  fillFlatTerrain(farBlocks, 10, 64, 10, "stone");
  farBlocks.set("30,64,10", { name: "crafting_table", position: vec3(30, 64, 10), boundingBox: "block" });
  const botFar = createCraftingMockBot({
    items: { oak_planks: 2, iron_ingot: 8 },
    blocks: farBlocks,
    pos: vec3(10, 64, 10),
  });
  const resNoTable = await executeCraftAction(botFar, { item_name: "iron_chestplate", count: 1 });
  assert.equal(resNoTable.ok, false);
  assert.equal(resNoTable.reason, "no_crafting_table");
  assert.equal(botFar.gotoCalls.length, 0);
  assert.deepEqual(resNoTable.missing_materials, [
    { item: "oak_planks", need: 4, have: 2, shortfall: 2 },
  ]);
});

test("craft reports exact missing_materials without wasting/placing crafting_table, and handles partial crafts", async () => {
  const blocks = new Map();
  fillFlatTerrain(blocks, 10, 64, 10, "stone");
  const bot = createCraftingMockBot({
    items: { diamond: 1, stick: 1, crafting_table: 1 },
    blocks,
    pos: vec3(10, 64, 10),
  });

  const miss = await executeCraftAction(bot, { item_name: "diamond_pickaxe", count: 1 });
  assert.equal(miss.ok, false);
  assert.equal(miss.reason, "missing_materials");
  assert.equal(bot.placeCalls.length, 0);
  assert.deepEqual(miss.missing_materials, [
    { item: "diamond", need: 3, have: 1, shortfall: 2 },
    { item: "stick", need: 2, have: 1, shortfall: 1 },
  ]);

  // Partial craft: enough for 1 iron_sword, requested 3
  const bot2 = createCraftingMockBot({
    items: { iron_ingot: 2, stick: 2, crafting_table: 1 },
    blocks,
    pos: vec3(10, 64, 10),
  });
  const partial = await executeCraftAction(bot2, { item_name: "iron_sword", count: 3 });
  assert.equal(partial.ok, true);
  assert.equal(partial.partial, true);
  assert.equal(partial.crafted_count, 1);
  assert.equal(partial.requested_count, 3);
  assert.ok(partial.equipped_items.includes("iron_sword"));
  assert.deepEqual(partial.missing_materials, [
    { item: "iron_ingot", need: 4, have: 0, shortfall: 4 },
    { item: "stick", need: 2, have: 1, shortfall: 1 },
  ]);
});

test("WS integration: craft RPC dispatches end-to-end and returns snapshot + missing_materials", async () => {
  const blocks = new Map();
  fillFlatTerrain(blocks, 10, 64, 10, "stone");
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
      const bot = createCraftingMockBot({
        items: { oak_log: 2, iron_ingot: 2 },
        blocks,
        pos: vec3(10, 64, 10),
      });
      queueMicrotask(() => bot.emit("spawn"));
      return bot;
    },
    snapshotIntervalMs: 5000,
    survivalTickMs: 5000,
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

    const bad = await rpc("c-bad", "mc.dispatch_goal", {
      action_type: "craft",
      item_name: "iron_sword",
      count: 20,
    });
    assert.equal(bad.data.ok, false);
    assert.equal(bad.data.accepted, false);
    assert.equal(bad.data.reason, "invalid_craft_count");

    const ok = await rpc("c-ok", "mc.dispatch_goal", {
      action_type: "craft",
      item_name: "iron_sword",
      count: 1,
    });
    assert.equal(ok.ok, true);
    assert.equal(ok.data.ok, true);
    assert.equal(ok.data.crafted_count, 1);
    assert.equal(ok.data.used_table, true);
    assert.equal(ok.data.snapshot.equipment.main_hand, "iron_sword");

    const noRecipe = await rpc("c-norec", "mc.dispatch_goal", {
      action_type: "craft",
      item_name: "bedrock",
      count: 1,
    });
    assert.equal(noRecipe.data.ok, false);
    assert.equal(noRecipe.data.reason, "no_recipe");

    ws.close();
  } finally {
    await server.close();
  }
});
