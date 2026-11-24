import vec3 from "vec3";
import pathfinderPkg from "mineflayer-pathfinder";
import { normalizeCanonicalItemId } from "./protocol.mjs";
import { extractCanonicalItemName, getInventoryItems } from "./survival.mjs";
import { countInventoryByName, executeCraftAction, getPlanksForLog } from "./crafting.mjs";

const { goals } = pathfinderPkg;
const { GoalNear } = goals;

export const MIN_SMELT_COUNT = 1;
export const MAX_SMELT_COUNT = 16;
export const FURNACE_SEARCH_RADIUS = 16;
export const FURNACE_PLACE_RADIUS = 2;

export const SMELTABLE_ITEMS = Object.freeze({
  raw_iron: Object.freeze({ output: "iron_ingot", category: "ore" }),
  raw_gold: Object.freeze({ output: "gold_ingot", category: "ore" }),
  raw_copper: Object.freeze({ output: "copper_ingot", category: "ore" }),
  iron_ore: Object.freeze({ output: "iron_ingot", category: "ore" }),
  deepslate_iron_ore: Object.freeze({ output: "iron_ingot", category: "ore" }),
  gold_ore: Object.freeze({ output: "gold_ingot", category: "ore" }),
  deepslate_gold_ore: Object.freeze({ output: "gold_ingot", category: "ore" }),
  copper_ore: Object.freeze({ output: "copper_ingot", category: "ore" }),
  deepslate_copper_ore: Object.freeze({ output: "copper_ingot", category: "ore" }),
  ancient_debris: Object.freeze({ output: "netherite_scrap", category: "ore" }),
  beef: Object.freeze({ output: "cooked_beef", category: "food" }),
  porkchop: Object.freeze({ output: "cooked_porkchop", category: "food" }),
  mutton: Object.freeze({ output: "cooked_mutton", category: "food" }),
  chicken: Object.freeze({ output: "cooked_chicken", category: "food" }),
  rabbit: Object.freeze({ output: "cooked_rabbit", category: "food" }),
  cod: Object.freeze({ output: "cooked_cod", category: "food" }),
  salmon: Object.freeze({ output: "cooked_salmon", category: "food" }),
  potato: Object.freeze({ output: "baked_potato", category: "food" }),
  kelp: Object.freeze({ output: "dried_kelp", category: "food" }),
  cobblestone: Object.freeze({ output: "stone", category: "block" }),
  sand: Object.freeze({ output: "glass", category: "block" }),
  red_sand: Object.freeze({ output: "glass", category: "block" }),
  oak_log: Object.freeze({ output: "charcoal", category: "wood" }),
  spruce_log: Object.freeze({ output: "charcoal", category: "wood" }),
  birch_log: Object.freeze({ output: "charcoal", category: "wood" }),
  jungle_log: Object.freeze({ output: "charcoal", category: "wood" }),
  acacia_log: Object.freeze({ output: "charcoal", category: "wood" }),
  dark_oak_log: Object.freeze({ output: "charcoal", category: "wood" }),
  mangrove_log: Object.freeze({ output: "charcoal", category: "wood" }),
  cherry_log: Object.freeze({ output: "charcoal", category: "wood" }),
});

const FUEL_CAPACITY_TABLE = Object.freeze({
  coal: 8,
  charcoal: 8,
  coal_block: 80,
  blaze_rod: 12,
  dried_kelp_block: 20,
  stick: 0.5,
  bamboo: 0.25,
});

const REPLACEABLE_BLOCKS = new Set([
  "air",
  "cave_air",
  "void_air",
  "short_grass",
  "grass",
  "tall_grass",
  "fern",
  "dead_bush",
  "snow",
]);

const UNSAFE_GROUND_BLOCKS = new Set([
  "air",
  "cave_air",
  "void_air",
  "water",
  "lava",
  "fire",
  "soul_fire",
  "cactus",
  "magma_block",
  "sweet_berry_bush",
  "campfire",
  "soul_campfire",
  "powder_snow",
  "lily_pad",
  "scaffolding",
  "short_grass",
  "grass",
  "tall_grass",
  "fern",
  "dead_bush",
  "snow",
]);

const PLACE_OFFSETS_XZ = [
  [1, 0],
  [-1, 0],
  [0, 1],
  [0, -1],
  [1, 1],
  [1, -1],
  [-1, 1],
  [-1, -1],
  [2, 0],
  [-2, 0],
  [0, 2],
  [0, -2],
];

export function parseSmeltParams(params) {
  const rawItem = params?.item_name ?? params?.item;
  if (typeof rawItem !== "string") {
    return { error: "invalid_item_name" };
  }
  const itemName = normalizeCanonicalItemId(rawItem);
  if (!itemName) {
    return { error: "invalid_item_name" };
  }

  const rawCount = params?.count === undefined ? 1 : params.count;
  if (
    typeof rawCount !== "number" ||
    !Number.isInteger(rawCount) ||
    rawCount < MIN_SMELT_COUNT ||
    rawCount > MAX_SMELT_COUNT
  ) {
    return { error: "invalid_smelt_count" };
  }

  let fuelName = "";
  if (params?.fuel_name !== undefined && params.fuel_name !== null && params.fuel_name !== "") {
    if (typeof params.fuel_name !== "string") {
      return { error: "invalid_fuel_name" };
    }
    const cleanFuel = normalizeCanonicalItemId(params.fuel_name);
    if (!cleanFuel) {
      return { error: "invalid_fuel_name" };
    }
    fuelName = cleanFuel;
  }

  return { itemName, count: rawCount, fuelName };
}

export function getFuelCapacity(itemName) {
  const clean = normalizeCanonicalItemId(itemName);
  if (!clean) return 0;
  if (Object.hasOwn(FUEL_CAPACITY_TABLE, clean)) {
    return FUEL_CAPACITY_TABLE[clean];
  }
  if (clean.endsWith("_planks") && !clean.startsWith("crimson_") && !clean.startsWith("warped_")) {
    return 1.5;
  }
  if (clean.endsWith("_log") || clean.endsWith("_wood")) {
    return 1.5;
  }
  return 0;
}

export function pickBestFuel(countsMap, { preferredFuel = "", inputItemName = "", targetCount = 1 } = {}) {
  const availableForFuel = (name) => {
    const raw = countsMap.get(name) ?? 0;
    if (name === inputItemName) {
      return Math.max(0, raw - targetCount);
    }
    return raw;
  };

  if (preferredFuel) {
    const cap = getFuelCapacity(preferredFuel);
    const have = availableForFuel(preferredFuel);
    if (cap > 0 && have > 0) {
      return { fuelName: preferredFuel, capacity: cap, have };
    }
    return null;
  }

  const priorityRank = (name) => {
    if (name === "coal") return 1;
    if (name === "charcoal") return 2;
    if (name === "blaze_rod") return 3;
    if (name === "dried_kelp_block") return 4;
    if (name === "coal_block") return 5;
    if (name.endsWith("_planks")) return 6;
    if (name.endsWith("_log") || name.endsWith("_wood")) return 7;
    if (name === "stick") return 8;
    return 9;
  };

  let best = null;
  for (const [name] of countsMap.entries()) {
    const cap = getFuelCapacity(name);
    if (cap <= 0) continue;
    const have = availableForFuel(name);
    if (have <= 0) continue;
    const rank = priorityRank(name);
    if (!best || rank < best.rank || (rank === best.rank && have > best.have)) {
      best = { fuelName: name, capacity: cap, have, rank };
    }
  }
  if (!best) return null;
  return { fuelName: best.fuelName, capacity: best.capacity, have: best.have };
}

function allowedFurnaceNames(category) {
  if (category === "ore") return new Set(["blast_furnace", "furnace"]);
  if (category === "food") return new Set(["smoker", "furnace"]);
  return new Set(["furnace"]);
}

export function findNearbyFurnace(
  bot,
  category = "block",
  maxDistance = FURNACE_SEARCH_RADIUS,
  { isUnreachableFn = null } = {},
) {
  const origin = bot?.entity?.position;
  if (!origin) return null;
  const allowed = allowedFurnaceNames(category);

  const distOf = (p) => {
    if (typeof origin.distanceTo === "function") return origin.distanceTo(p);
    return Math.hypot((p.x ?? 0) - (origin.x ?? 0), (p.y ?? 0) - (origin.y ?? 0), (p.z ?? 0) - (origin.z ?? 0));
  };

  if (typeof bot.findBlocks === "function") {
    const positions =
      bot.findBlocks({
        matching: (b) => Boolean(b && allowed.has(b.name)),
        point: origin,
        maxDistance,
        count: 6,
      }) || [];
    let best = null;
    for (const p of positions) {
      const block = typeof bot.blockAt === "function" ? bot.blockAt(p) : null;
      if (!block || !allowed.has(block.name)) continue;
      const pos = block.position ?? p;
      if (typeof isUnreachableFn === "function" && isUnreachableFn(pos)) continue;
      const dist = distOf(pos);
      if (Number.isFinite(dist) && dist <= maxDistance && (!best || dist < best.dist)) {
        best = { block, pos, dist };
      }
    }
    if (best) return best;
  }

  if (typeof bot.findBlock === "function") {
    const block = bot.findBlock({
      matching: (b) => Boolean(b && allowed.has(b.name)),
      maxDistance,
    });
    if (block && allowed.has(block.name) && block.position) {
      if (typeof isUnreachableFn === "function" && isUnreachableFn(block.position)) {
        return null;
      }
      const dist = distOf(block.position);
      if (Number.isFinite(dist) && dist <= maxDistance) {
        return { block, pos: block.position, dist };
      }
    }
  }
  return null;
}

function isReplaceableBlock(block) {
  if (!block) return false;
  return REPLACEABLE_BLOCKS.has(normalizeCanonicalItemId(block.name));
}

function isSafeSolidGround(block) {
  if (!block) return false;
  const name = normalizeCanonicalItemId(block.name);
  if (!name || UNSAFE_GROUND_BLOCKS.has(name)) return false;
  if (block.boundingBox && block.boundingBox !== "block") return false;
  return true;
}

export async function placeFurnaceSafely(bot) {
  const origin = bot?.entity?.position;
  if (
    !origin ||
    typeof bot.blockAt !== "function" ||
    typeof bot.placeBlock !== "function" ||
    typeof bot.equip !== "function"
  ) {
    return { ok: false, reason: "furnace_placement_failed" };
  }

  const furnaceInvItem = getInventoryItems(bot).find(
    (it) => extractCanonicalItemName(it) === "furnace",
  );
  if (!furnaceInvItem) {
    return { ok: false, reason: "no_furnace" };
  }

  const bx = Math.floor(origin.x ?? 0);
  const by = Math.floor(origin.y ?? 0);
  const bz = Math.floor(origin.z ?? 0);

  for (const [dx, dz] of PLACE_OFFSETS_XZ) {
    if (Math.hypot(dx, dz) > FURNACE_PLACE_RADIUS) continue;
    for (const dy of [0, -1, 1]) {
      const tx = bx + dx;
      const ty = by + dy;
      const tz = bz + dz;
      if (tx === bx && tz === bz && (ty === by || ty === by + 1)) continue;

      const targetPos = vec3(tx, ty, tz);
      const groundPos = vec3(tx, ty - 1, tz);
      const targetBlock = bot.blockAt(targetPos);
      const groundBlock = bot.blockAt(groundPos);
      if (!isReplaceableBlock(targetBlock) || !isSafeSolidGround(groundBlock)) {
        continue;
      }

      try {
        await bot.equip(furnaceInvItem, "hand");
        await bot.placeBlock(groundBlock, vec3(0, 1, 0));
        const placedBlock = bot.blockAt(targetPos);
        if (placedBlock && placedBlock.name === "furnace") {
          return { ok: true, furnaceBlock: placedBlock, placed: true };
        }
        const furnaceBlockId = bot.registry?.blocksByName?.furnace?.id ?? 61;
        return {
          ok: true,
          furnaceBlock: { name: "furnace", position: targetPos, type: furnaceBlockId },
          placed: true,
        };
      } catch {
        // Try next candidate spot
      }
    }
  }

  return { ok: false, reason: "furnace_placement_failed" };
}

function sleepAbortable(ms, signal) {
  if (signal?.aborted) return Promise.resolve(false);
  return new Promise((resolve) => {
    const onAbort = () => {
      clearTimeout(timer);
      resolve(false);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener?.("abort", onAbort);
      resolve(true);
    }, Math.max(5, ms));
    signal?.addEventListener?.("abort", onAbort, { once: true });
  });
}

function resolveItemTypeId(bot, itemName) {
  const fromReg = bot?.registry?.itemsByName?.[itemName]?.id;
  if (Number.isInteger(fromReg)) return fromReg;
  const invItem = getInventoryItems(bot).find((it) => extractCanonicalItemName(it) === itemName);
  if (invItem && Number.isInteger(invItem.type)) return invItem.type;
  return null;
}

export async function executeSmeltAction(
  bot,
  params,
  {
    gotoFn = null,
    signal = null,
    pollIntervalMs = 350,
    maxSmeltWaitMs = 24_000,
    isUnreachableFn = null,
    markUnreachableFn = null,
  } = {},
) {
  const parsed = parseSmeltParams(params);
  if (parsed.error) {
    return {
      ok: false,
      accepted: false,
      reason: parsed.error,
      summary:
        parsed.error === "invalid_item_name"
          ? "熔炼物品ID无效"
          : parsed.error === "invalid_fuel_name"
            ? "燃料物品ID无效"
            : "熔炼数量需为1到16的整数",
    };
  }

  const { itemName, count: requestedCount, fuelName: preferredFuel } = parsed;
  const spec = Object.hasOwn(SMELTABLE_ITEMS, itemName) ? SMELTABLE_ITEMS[itemName] : null;
  if (!spec) {
    return {
      ok: false,
      accepted: true,
      reason: "not_smeltable",
      item_name: itemName,
      output_item: "",
      requested_count: requestedCount,
      smelted_count: 0,
      missing_materials: [],
      summary: `该物品不可熔炼: ${itemName}`,
    };
  }

  if (typeof bot?.openFurnace !== "function") {
    return {
      ok: false,
      accepted: true,
      reason: "smelt_unavailable",
      item_name: itemName,
      output_item: spec.output,
      requested_count: requestedCount,
      smelted_count: 0,
      missing_materials: [],
      summary: "当前连接不支持打开熔炉",
    };
  }

  const countsInitial = countInventoryByName(bot);
  const haveInput = countsInitial.get(itemName) ?? 0;
  if (haveInput <= 0) {
    return {
      ok: false,
      accepted: true,
      reason: "missing_materials",
      item_name: itemName,
      output_item: spec.output,
      requested_count: requestedCount,
      smelted_count: 0,
      missing_materials: [{ item: itemName, need: requestedCount, have: 0, shortfall: requestedCount }],
      summary: `缺少待熔炼原料: ${itemName} x${requestedCount}`,
    };
  }

  const targetCount = Math.min(requestedCount, haveInput);

  // Convert 1 spare log into planks if no direct fuel is available and no explicit fuel was requested
  let counts = countInventoryByName(bot);
  let fuel = pickBestFuel(counts, { preferredFuel, inputItemName: itemName, targetCount });
  if (!fuel && !preferredFuel && typeof bot.craft === "function" && typeof bot.recipesFor === "function") {
    for (const [name, qty] of counts.entries()) {
      const plankName = getPlanksForLog(name);
      if (!plankName) continue;
      const spare = name === itemName ? qty - targetCount : qty;
      if (spare <= 0) continue;
      const plankItem = bot.registry?.itemsByName?.[plankName];
      if (!plankItem) continue;
      const recipes = bot.recipesFor(plankItem.id, null, 1, null) || [];
      if (recipes.length > 0) {
        try {
          await bot.craft(recipes[0], 1, null);
          break;
        } catch {
          // Try next log species
        }
      }
    }
    counts = countInventoryByName(bot);
    fuel = pickBestFuel(counts, { preferredFuel, inputItemName: itemName, targetCount });
  }

  if (!fuel) {
    const missingFuel = preferredFuel || "coal";
    return {
      ok: false,
      accepted: true,
      reason: "missing_materials",
      item_name: itemName,
      output_item: spec.output,
      requested_count: requestedCount,
      smelted_count: 0,
      missing_materials: [{ item: missingFuel, need: 1, have: 0, shortfall: 1 }],
      summary: `缺少熔炉燃料: ${missingFuel} x1`,
    };
  }

  if (signal?.aborted) return { aborted: true };

  // Locate existing reachable furnace within 16 blocks or craft/place one safely within 2 blocks
  let furnaceBlock = null;
  const nearby = findNearbyFurnace(bot, spec.category, FURNACE_SEARCH_RADIUS, { isUnreachableFn });
  if (nearby) {
    let reachedNearby = true;
    if (nearby.dist > 3.5 && typeof gotoFn === "function") {
      try {
        await gotoFn(new GoalNear(nearby.pos.x, nearby.pos.y, nearby.pos.z, 2), signal);
        if (signal?.aborted) return { aborted: true };
      } catch {
        if (signal?.aborted) return { aborted: true };
        reachedNearby = false;
        if (typeof markUnreachableFn === "function") {
          markUnreachableFn(nearby.pos);
        }
      }
    }
    if (reachedNearby) {
      furnaceBlock = nearby.block;
    }
  }
  if (!furnaceBlock) {
    let curCounts = countInventoryByName(bot);
    if ((curCounts.get("furnace") ?? 0) === 0) {
      const craftRes = await executeCraftAction(
        bot,
        { item_name: "furnace", count: 1 },
        { gotoFn, autoEquip: false, signal, isUnreachableFn, markUnreachableFn },
      );
      if (craftRes?.aborted || signal?.aborted) return { aborted: true };
      curCounts = countInventoryByName(bot);
      if ((curCounts.get("furnace") ?? 0) === 0) {
        const haveCobble = curCounts.get("cobblestone") ?? 0;
        const missing =
          Array.isArray(craftRes?.missing_materials) && craftRes.missing_materials.length > 0
            ? craftRes.missing_materials
            : haveCobble < 8
              ? [{ item: "cobblestone", need: 8, have: haveCobble, shortfall: 8 - haveCobble }]
              : [{ item: "furnace", need: 1, have: 0, shortfall: 1 }];
        return {
          ok: false,
          accepted: true,
          reason: "no_furnace",
          item_name: itemName,
          output_item: spec.output,
          requested_count: requestedCount,
          smelted_count: 0,
          missing_materials: missing,
          summary: "附近16格内无熔炉且材料不足以制作熔炉",
        };
      }
    }

    const placed = await placeFurnaceSafely(bot);
    if (!placed.ok) {
      return {
        ok: false,
        accepted: true,
        reason: placed.reason,
        item_name: itemName,
        output_item: spec.output,
        requested_count: requestedCount,
        smelted_count: 0,
        missing_materials: [],
        summary: "周围2格内未找到安全实心地面放置熔炉",
      };
    }
    furnaceBlock = placed.furnaceBlock;
  }

  const inputTypeId = resolveItemTypeId(bot, itemName);
  const fuelTypeId = resolveItemTypeId(bot, fuel.fuelName);
  if (inputTypeId === null || fuelTypeId === null) {
    return {
      ok: false,
      accepted: true,
      reason: "smelt_failed",
      item_name: itemName,
      output_item: spec.output,
      requested_count: requestedCount,
      smelted_count: 0,
      missing_materials: [],
      summary: `无法解析熔炼物品ID: ${itemName}`,
    };
  }

  const fuelNeeded = Math.min(fuel.have, Math.max(1, Math.ceil(targetCount / fuel.capacity)));
  let smeltedCount = 0;
  let furnace = null;

  try {
    furnace = await bot.openFurnace(furnaceBlock);
    if (signal?.aborted) return { aborted: true };

    // Collect any pre-existing output before loading new items
    const existingOut = typeof furnace.outputItem === "function" ? furnace.outputItem() : null;
    if (existingOut && typeof furnace.takeOutput === "function") {
      try {
        await furnace.takeOutput();
      } catch {
        // Ignore stale output collection errors
      }
    }

    if (typeof furnace.putFuel === "function") {
      await furnace.putFuel(fuelTypeId, null, fuelNeeded);
    }
    if (typeof furnace.putInput === "function") {
      await furnace.putInput(inputTypeId, null, targetCount);
    }

    const deadlineAt = Date.now() + Math.max(200, maxSmeltWaitMs);
    while (smeltedCount < targetCount && !signal?.aborted && Date.now() < deadlineAt) {
      const outItem = typeof furnace.outputItem === "function" ? furnace.outputItem() : null;
      const outCount = Number(outItem?.count ?? 0);
      const inItem = typeof furnace.inputItem === "function" ? furnace.inputItem() : null;
      const inCount = Number(inItem?.count ?? 0);

      if (outCount > 0 && (outCount + smeltedCount >= targetCount || inCount <= 0)) {
        if (typeof furnace.takeOutput === "function") {
          const taken = await furnace.takeOutput();
          const takenCnt = Number(taken?.count ?? outCount);
          smeltedCount += Number.isFinite(takenCnt) && takenCnt > 0 ? Math.floor(takenCnt) : outCount;
        } else {
          smeltedCount += Math.floor(outCount);
        }
        if (smeltedCount >= targetCount || inCount <= 0) break;
      }

      const waited = await sleepAbortable(pollIntervalMs, signal);
      if (!waited) break;
    }

    // Drain any ready output even if timed out or aborted mid-batch
    const finalOut = typeof furnace.outputItem === "function" ? furnace.outputItem() : null;
    const finalOutCount = Number(finalOut?.count ?? 0);
    if (finalOutCount > 0 && typeof furnace.takeOutput === "function") {
      try {
        const taken = await furnace.takeOutput();
        const takenCnt = Number(taken?.count ?? finalOutCount);
        smeltedCount += Number.isFinite(takenCnt) && takenCnt > 0 ? Math.floor(takenCnt) : finalOutCount;
      } catch {
        // Ignore best-effort drain errors
      }
    }
  } catch {
    if (smeltedCount === 0) {
      return {
        ok: false,
        accepted: true,
        reason: "smelt_failed",
        item_name: itemName,
        output_item: spec.output,
        requested_count: requestedCount,
        smelted_count: 0,
        missing_materials: [],
        summary: `熔炼${itemName}失败`,
      };
    }
  } finally {
    if (furnace && typeof furnace.close === "function") {
      try {
        furnace.close();
      } catch {
        // Ignore furnace close errors
      }
    }
  }

  if (smeltedCount <= 0) {
    if (signal?.aborted) return { aborted: true };
    return {
      ok: false,
      accepted: true,
      reason: "smelt_timeout",
      item_name: itemName,
      output_item: spec.output,
      requested_count: requestedCount,
      smelted_count: 0,
      used_fuel: fuel.fuelName,
      used_furnace: true,
      missing_materials: [],
      summary: `熔炼${itemName}等待超时，暂未产出${spec.output}`,
    };
  }

  const partial = smeltedCount < requestedCount;
  const summary = partial
    ? `已部分熔炼出 ${spec.output} x${smeltedCount}/${requestedCount}（燃料:${fuel.fuelName}）`
    : `已熔炼出 ${spec.output} x${smeltedCount}（燃料:${fuel.fuelName}）`;
  const furnacePos = furnaceBlock?.position
    ? [
        Math.floor(furnaceBlock.position.x),
        Math.floor(furnaceBlock.position.y),
        Math.floor(furnaceBlock.position.z),
      ]
    : undefined;

  return {
    ok: true,
    accepted: true,
    item_name: itemName,
    output_item: spec.output,
    requested_count: requestedCount,
    smelted_count: smeltedCount,
    partial,
    used_fuel: fuel.fuelName,
    used_furnace: true,
    ...(furnacePos ? { furnace_pos: furnacePos } : {}),
    missing_materials: [],
    summary,
  };
}
