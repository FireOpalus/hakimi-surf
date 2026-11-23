import vec3 from "vec3";
import pathfinderPkg from "mineflayer-pathfinder";
import { normalizeCanonicalItemId } from "./protocol.mjs";
import {
  autoEquipBestGear,
  equipBestWeapon,
  extractCanonicalItemName,
  getInventoryItems,
  getWeaponScore,
} from "./survival.mjs";

const { goals } = pathfinderPkg;
const { GoalNear } = goals;

export const MIN_CRAFT_COUNT = 1;
export const MAX_CRAFT_COUNT = 16;
export const CRAFTING_TABLE_SEARCH_RADIUS = 16;
export const CRAFTING_TABLE_PLACE_RADIUS = 2;
export const MAX_WOOD_PREPROCESS_DEPTH = 2;

const WOOD_SPECIES = [
  "oak",
  "spruce",
  "birch",
  "jungle",
  "acacia",
  "dark_oak",
  "mangrove",
  "cherry",
  "pale_oak",
];

const LOG_TO_PLANKS = new Map();
for (const species of WOOD_SPECIES) {
  LOG_TO_PLANKS.set(`${species}_log`, `${species}_planks`);
  LOG_TO_PLANKS.set(`stripped_${species}_log`, `${species}_planks`);
  LOG_TO_PLANKS.set(`${species}_wood`, `${species}_planks`);
  LOG_TO_PLANKS.set(`stripped_${species}_wood`, `${species}_planks`);
}
LOG_TO_PLANKS.set("crimson_stem", "crimson_planks");
LOG_TO_PLANKS.set("stripped_crimson_stem", "crimson_planks");
LOG_TO_PLANKS.set("crimson_hyphae", "crimson_planks");
LOG_TO_PLANKS.set("stripped_crimson_hyphae", "crimson_planks");
LOG_TO_PLANKS.set("warped_stem", "warped_planks");
LOG_TO_PLANKS.set("stripped_warped_stem", "warped_planks");
LOG_TO_PLANKS.set("warped_hyphae", "warped_planks");
LOG_TO_PLANKS.set("stripped_warped_hyphae", "warped_planks");
LOG_TO_PLANKS.set("bamboo_block", "bamboo_planks");
LOG_TO_PLANKS.set("stripped_bamboo_block", "bamboo_planks");

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

export function parseCraftParams(params) {
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
    rawCount < MIN_CRAFT_COUNT ||
    rawCount > MAX_CRAFT_COUNT
  ) {
    return { error: "invalid_craft_count" };
  }
  return { itemName, count: rawCount };
}

export function getPlanksForLog(logName) {
  return LOG_TO_PLANKS.get(normalizeCanonicalItemId(logName)) ?? "";
}

export function isPlanksName(itemName) {
  return normalizeCanonicalItemId(itemName).endsWith("_planks");
}

export function countInventoryByName(bot) {
  const counts = new Map();
  for (const item of getInventoryItems(bot)) {
    const name = extractCanonicalItemName(item);
    if (!name) continue;
    const c = Number(item.count ?? 1);
    if (!Number.isFinite(c) || c <= 0) continue;
    counts.set(name, (counts.get(name) ?? 0) + Math.floor(c));
  }
  return counts;
}

function countAllPlanks(countsMap) {
  let total = 0;
  for (const [name, count] of countsMap.entries()) {
    if (name.endsWith("_planks")) total += count;
  }
  return total;
}

function resolveItemNameById(bot, id) {
  if (id === null || id === undefined) return "";
  if (typeof id === "string") return normalizeCanonicalItemId(id);
  const entry = bot?.registry?.items?.[id];
  if (entry && typeof entry.name === "string") {
    return normalizeCanonicalItemId(entry.name);
  }
  return "";
}

export function getRecipeResultCount(recipe) {
  if (recipe?.result && Number.isInteger(recipe.result.count) && recipe.result.count > 0) {
    return recipe.result.count;
  }
  if (Array.isArray(recipe?.delta)) {
    for (const d of recipe.delta) {
      if (d && typeof d.count === "number" && d.count > 0) {
        return Math.floor(d.count);
      }
    }
  }
  return 1;
}

export function extractRecipeIngredients(recipe, bot) {
  const reqs = new Map();
  if (!recipe || typeof recipe !== "object") return reqs;

  const add = (name, qty) => {
    const clean = normalizeCanonicalItemId(name);
    if (!clean || !Number.isFinite(qty) || qty <= 0) return;
    reqs.set(clean, (reqs.get(clean) ?? 0) + Math.ceil(qty));
  };

  if (Array.isArray(recipe.delta)) {
    for (const d of recipe.delta) {
      if (!d || typeof d.count !== "number" || d.count >= 0) continue;
      const name = d.name ? normalizeCanonicalItemId(d.name) : resolveItemNameById(bot, d.id);
      add(name, -d.count);
    }
    if (reqs.size > 0) return reqs;
  }

  if (Array.isArray(recipe.ingredients)) {
    for (const ing of recipe.ingredients) {
      if (ing === null || ing === undefined) continue;
      if (typeof ing === "number") {
        add(resolveItemNameById(bot, ing), 1);
      } else if (typeof ing === "object") {
        const name = ing.name ? normalizeCanonicalItemId(ing.name) : resolveItemNameById(bot, ing.id);
        const qty = Math.abs(Number(ing.count ?? 1));
        add(name, qty || 1);
      }
    }
    if (reqs.size > 0) return reqs;
  }

  if (Array.isArray(recipe.inShape)) {
    for (const row of recipe.inShape) {
      if (!Array.isArray(row)) continue;
      for (const cell of row) {
        if (cell === null || cell === undefined) continue;
        if (typeof cell === "number") {
          add(resolveItemNameById(bot, cell), 1);
        } else if (typeof cell === "object") {
          const name = cell.name
            ? normalizeCanonicalItemId(cell.name)
            : resolveItemNameById(bot, cell.id);
          add(name, 1);
        }
      }
    }
  }

  return reqs;
}

function getAllRecipesSafe(bot, itemId, tableHint) {
  if (typeof bot?.recipesAll === "function") {
    const res = bot.recipesAll(itemId, null, tableHint);
    return Array.isArray(res) ? res : [];
  }
  if (typeof bot?.recipesFor === "function") {
    const res = bot.recipesFor(itemId, null, 1, tableHint);
    return Array.isArray(res) ? res : [];
  }
  return [];
}

function getCraftableRecipesSafe(bot, itemId, tableBlock) {
  if (typeof bot?.recipesFor !== "function") return [];
  const res = bot.recipesFor(itemId, null, 1, tableBlock);
  return Array.isArray(res) ? res : [];
}

function estimateVirtualCountsWithWood(countsMap) {
  const virtual = new Map(countsMap);
  let totalAddedPlanks = 0;
  for (const [name, count] of countsMap.entries()) {
    const plankName = LOG_TO_PLANKS.get(name);
    if (!plankName || count <= 0) continue;
    const yieldPerLog = name.includes("bamboo_block") ? 2 : 4;
    const added = count * yieldPerLog;
    virtual.set(plankName, (virtual.get(plankName) ?? 0) + added);
    totalAddedPlanks += added;
  }
  const totalPlanksAvailable = countAllPlanks(virtual);
  if (totalPlanksAvailable >= 2) {
    const possibleSticks = Math.floor(totalPlanksAvailable / 2) * 4;
    virtual.set("stick", (virtual.get("stick") ?? 0) + possibleSticks);
  }
  virtual._totalAddedPlanks = totalAddedPlanks;
  return virtual;
}

function evaluateRecipeShortfall(recipe, bot, remainingCount, countsMap) {
  const perBatch = getRecipeResultCount(recipe);
  const batches = Math.max(1, Math.ceil(remainingCount / perBatch));
  const reqs = extractRecipeIngredients(recipe, bot);
  const missing = [];
  let totalShortfall = 0;
  for (const [item, qtyPerBatch] of reqs.entries()) {
    const need = qtyPerBatch * batches;
    const have = countsMap.get(item) ?? 0;
    const shortfall = Math.max(0, need - have);
    if (shortfall > 0) {
      missing.push({ item, need, have, shortfall });
      totalShortfall += shortfall;
    }
  }
  missing.sort((a, b) => a.item.localeCompare(b.item));
  return { recipe, batches, reqs, missing, totalShortfall };
}

export function pickBestRecipeAnalysis(recipes, bot, remainingCount, countsMap) {
  let best = null;
  for (const recipe of recipes) {
    const analysis = evaluateRecipeShortfall(recipe, bot, remainingCount, countsMap);
    if (!best) {
      best = analysis;
      continue;
    }
    if (analysis.totalShortfall < best.totalShortfall) {
      best = analysis;
    } else if (
      analysis.totalShortfall === best.totalShortfall &&
      analysis.missing.length < best.missing.length
    ) {
      best = analysis;
    } else if (
      analysis.totalShortfall === best.totalShortfall &&
      analysis.missing.length === best.missing.length &&
      Boolean(best.recipe.requiresTable) &&
      !analysis.recipe.requiresTable
    ) {
      best = analysis;
    }
  }
  return best;
}

async function craftPlanksFromLogs(bot, { preferredPlankName = "", planksNeeded = 0, signal = null }) {
  if (planksNeeded <= 0 || signal?.aborted || typeof bot.craft !== "function") return 0;
  let craftedPlanks = 0;

  const counts = countInventoryByName(bot);
  const candidates = [];
  for (const [logName, count] of counts.entries()) {
    const plankName = LOG_TO_PLANKS.get(logName);
    if (!plankName || count <= 0) continue;
    candidates.push({
      logName,
      plankName,
      logCount: count,
      priority: preferredPlankName && plankName === preferredPlankName ? 0 : 1,
    });
  }
  candidates.sort((a, b) => a.priority - b.priority || b.logCount - a.logCount || a.logName.localeCompare(b.logName));

  for (const cand of candidates) {
    if (craftedPlanks >= planksNeeded || signal?.aborted) break;
    if (preferredPlankName && cand.plankName !== preferredPlankName && candidates.some((c) => c.plankName === preferredPlankName && c.logCount > 0)) {
      continue;
    }
    const plankItem = bot.registry?.itemsByName?.[cand.plankName];
    if (!plankItem) continue;
    const recipes = getCraftableRecipesSafe(bot, plankItem.id, null);
    if (recipes.length === 0) continue;
    const recipe = recipes[0];
    const perBatch = getRecipeResultCount(recipe);
    const remainingPlanks = planksNeeded - craftedPlanks;
    const batchesNeeded = Math.ceil(remainingPlanks / perBatch);
    const batches = Math.min(batchesNeeded, cand.logCount);
    if (batches <= 0) continue;
    try {
      await bot.craft(recipe, batches, null);
      craftedPlanks += batches * perBatch;
    } catch {
      // Try next wood species if this batch failed
    }
  }
  return craftedPlanks;
}

async function craftSticksFromPlanks(bot, { sticksNeeded = 0, signal = null }) {
  if (sticksNeeded <= 0 || signal?.aborted || typeof bot.craft !== "function") return 0;
  const stickItem = bot.registry?.itemsByName?.stick;
  if (!stickItem) return 0;

  let craftedSticks = 0;
  while (craftedSticks < sticksNeeded && !signal?.aborted) {
    const recipes = getCraftableRecipesSafe(bot, stickItem.id, null);
    if (recipes.length === 0) break;
    const recipe = recipes[0];
    const perBatch = getRecipeResultCount(recipe);
    const remaining = sticksNeeded - craftedSticks;
    const counts = countInventoryByName(bot);
    const reqs = extractRecipeIngredients(recipe, bot);
    let maxBatches = Math.ceil(remaining / perBatch);
    for (const [ingName, qty] of reqs.entries()) {
      const have = counts.get(ingName) ?? 0;
      maxBatches = Math.min(maxBatches, Math.floor(have / qty));
    }
    if (maxBatches <= 0) break;
    try {
      await bot.craft(recipe, maxBatches, null);
      craftedSticks += maxBatches * perBatch;
    } catch {
      break;
    }
  }
  return craftedSticks;
}

/**
 * Preprocess wood ingredients up to 2 levels:
 *   Level 1: *_log -> *_planks
 *   Level 2: *_planks -> stick
 */
async function preprocessWoodMaterials(
  bot,
  { targetItemName, candidateRecipes, targetCount, needsTableCraft, signal },
) {
  if (signal?.aborted || candidateRecipes.length === 0) return;

  const countsBefore = countInventoryByName(bot);
  const virtualCounts = estimateVirtualCountsWithWood(countsBefore);
  const bestPlanned = pickBestRecipeAnalysis(candidateRecipes, bot, targetCount, virtualCounts);
  if (!bestPlanned) return;

  const { reqs, batches } = bestPlanned;
  const needSticks = targetItemName === "stick" ? 0 : (reqs.get("stick") ?? 0) * batches;
  const haveSticks = countsBefore.get("stick") ?? 0;
  const stickShortfall = Math.max(0, needSticks - haveSticks);
  const planksForSticks = stickShortfall > 0 ? Math.ceil(stickShortfall / 4) * 2 : 0;

  let specificPlankName = "";
  let specificPlankShortfall = 0;
  for (const [ingName, qtyPerBatch] of reqs.entries()) {
    if (isPlanksName(ingName)) {
      specificPlankName = ingName;
      const need = qtyPerBatch * batches;
      const have = countsBefore.get(ingName) ?? 0;
      specificPlankShortfall = Math.max(0, need - have);
      break;
    }
  }

  const planksForTable = needsTableCraft ? 4 : 0;
  const singleBatchPlanks = specificPlankName ? (reqs.get(specificPlankName) ?? 0) : 0;
  const singleBatchStickPlanks =
    targetItemName !== "stick" && (reqs.get("stick") ?? 0) > haveSticks ? 2 : 0;
  const minPlanksForOneWithTable = singleBatchPlanks + singleBatchStickPlanks + planksForTable;
  const totalPlanksNeed =
    (specificPlankName ? (reqs.get(specificPlankName) ?? 0) * batches : 0) +
    planksForSticks +
    planksForTable;
  const maxPossiblePlanks = countAllPlanks(countsBefore) + countPotentialPlanksFromLogs(countsBefore);
  // Avoid burning the last planks into sticks when total wood cannot even fund a crafting table + 1 item.
  if (needsTableCraft && maxPossiblePlanks < minPlanksForOneWithTable) {
    return;
  }

  // Level 1: log -> planks (skip if target itself is a planks item)
  if (!isPlanksName(targetItemName)) {
    if (specificPlankShortfall > 0) {
      await craftPlanksFromLogs(bot, {
        preferredPlankName: specificPlankName,
        planksNeeded: specificPlankShortfall,
        signal,
      });
    }
    const countsAfterSpecific = countInventoryByName(bot);
    const remainingGenericShortfall = Math.max(
      0,
      totalPlanksNeed - countAllPlanks(countsAfterSpecific),
    );
    if (remainingGenericShortfall > 0) {
      await craftPlanksFromLogs(bot, {
        preferredPlankName: "",
        planksNeeded: remainingGenericShortfall,
        signal,
      });
    }
  }

  // Level 2: planks -> stick (skip if target itself is stick)
  if (stickShortfall > 0 && targetItemName !== "stick") {
    await craftSticksFromPlanks(bot, {
      sticksNeeded: stickShortfall,
      signal,
    });
  }
}

export function findNearbyCraftingTable(bot, maxDistance = CRAFTING_TABLE_SEARCH_RADIUS) {
  const origin = bot?.entity?.position;
  if (!origin) return null;

  const distOf = (p) => {
    if (typeof origin.distanceTo === "function") return origin.distanceTo(p);
    return Math.hypot((p.x ?? 0) - (origin.x ?? 0), (p.y ?? 0) - (origin.y ?? 0), (p.z ?? 0) - (origin.z ?? 0));
  };

  if (typeof bot.findBlocks === "function") {
    const positions =
      bot.findBlocks({
        matching: (b) => Boolean(b && b.name === "crafting_table"),
        point: origin,
        maxDistance,
        count: 6,
      }) || [];
    let best = null;
    for (const p of positions) {
      const block = typeof bot.blockAt === "function" ? bot.blockAt(p) : null;
      if (!block || block.name !== "crafting_table") continue;
      const dist = distOf(block.position ?? p);
      if (Number.isFinite(dist) && dist <= maxDistance && (!best || dist < best.dist)) {
        best = { block, pos: block.position ?? p, dist };
      }
    }
    if (best) return best;
  }

  if (typeof bot.findBlock === "function") {
    const block = bot.findBlock({
      matching: (b) => Boolean(b && b.name === "crafting_table"),
      maxDistance,
    });
    if (block && block.name === "crafting_table" && block.position) {
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
  const name = normalizeCanonicalItemId(block.name);
  return REPLACEABLE_BLOCKS.has(name);
}

function isSafeSolidGround(block) {
  if (!block) return false;
  const name = normalizeCanonicalItemId(block.name);
  if (!name || UNSAFE_GROUND_BLOCKS.has(name)) return false;
  if (block.boundingBox && block.boundingBox !== "block") return false;
  return true;
}

export async function placeCraftingTableSafely(bot) {
  const origin = bot?.entity?.position;
  if (
    !origin ||
    typeof bot.blockAt !== "function" ||
    typeof bot.placeBlock !== "function" ||
    typeof bot.equip !== "function"
  ) {
    return { ok: false, reason: "table_placement_failed" };
  }

  const tableInvItem = getInventoryItems(bot).find(
    (it) => extractCanonicalItemName(it) === "crafting_table",
  );
  if (!tableInvItem) {
    return { ok: false, reason: "no_crafting_table" };
  }

  const bx = Math.floor(origin.x ?? 0);
  const by = Math.floor(origin.y ?? 0);
  const bz = Math.floor(origin.z ?? 0);

  for (const [dx, dz] of PLACE_OFFSETS_XZ) {
    if (Math.hypot(dx, dz) > CRAFTING_TABLE_PLACE_RADIUS) continue;
    for (const dy of [0, -1, 1]) {
      const tx = bx + dx;
      const ty = by + dy;
      const tz = bz + dz;
      // Never place inside bot's feet or head
      if (tx === bx && tz === bz && (ty === by || ty === by + 1)) continue;

      const targetPos = vec3(tx, ty, tz);
      const groundPos = vec3(tx, ty - 1, tz);
      const targetBlock = bot.blockAt(targetPos);
      const groundBlock = bot.blockAt(groundPos);
      if (!isReplaceableBlock(targetBlock) || !isSafeSolidGround(groundBlock)) {
        continue;
      }

      try {
        await bot.equip(tableInvItem, "hand");
        await bot.placeBlock(groundBlock, vec3(0, 1, 0));
        const placedBlock = bot.blockAt(targetPos);
        if (placedBlock && placedBlock.name === "crafting_table") {
          return { ok: true, tableBlock: placedBlock, placed: true };
        }
        const tableBlockId = bot.registry?.blocksByName?.crafting_table?.id ?? 58;
        return {
          ok: true,
          tableBlock: { name: "crafting_table", position: targetPos, type: tableBlockId },
          placed: true,
        };
      } catch {
        // Try next candidate spot
      }
    }
  }

  return { ok: false, reason: "table_placement_failed" };
}

function countPotentialPlanksFromLogs(countsMap) {
  let total = 0;
  for (const [name, count] of countsMap.entries()) {
    if (!LOG_TO_PLANKS.has(name) || count <= 0) continue;
    const yieldPerLog = name.includes("bamboo_block") ? 2 : 4;
    total += count * yieldPerLog;
  }
  return total;
}

/**
 * Compute upstream wood shortfall (planks & logs) when crafting fails due to
 * missing planks, sticks, or crafting_table wood so the planner knows to chop
 * logs instead of trying to craft sticks out of thin air.
 */
export function computeWoodShortfall(
  missing,
  countsMap,
  { needsTableCraft = false, recipePlanksForOne = 0 } = {},
) {
  let extraPlanksNeed = 0;
  for (const entry of missing || []) {
    if (!entry || typeof entry.shortfall !== "number" || entry.shortfall <= 0) continue;
    if (isPlanksName(entry.item)) {
      extraPlanksNeed += entry.shortfall;
    } else if (entry.item === "stick") {
      extraPlanksNeed += Math.ceil(entry.shortfall / 4) * 2;
    } else if (entry.item === "crafting_table") {
      extraPlanksNeed += entry.shortfall * 4;
    }
  }
  if (needsTableCraft && (countsMap.get("crafting_table") ?? 0) === 0) {
    const havePlanksNow = countAllPlanks(countsMap);
    const tableAndRecipeNeed = 4 + Math.max(0, recipePlanksForOne);
    const tableGap = Math.max(0, tableAndRecipeNeed - havePlanksNow);
    extraPlanksNeed = Math.max(extraPlanksNeed, tableGap);
  }
  const logPlanksAvailable = countPotentialPlanksFromLogs(countsMap);
  const netPlanksShortfall = Math.max(0, extraPlanksNeed - logPlanksAvailable);
  return {
    wood_shortfall_planks: netPlanksShortfall,
    wood_shortfall_logs: netPlanksShortfall > 0 ? Math.ceil(netPlanksShortfall / 4) : 0,
  };
}

function formatMissingSummary(missing, woodInfo = null) {
  const base =
    !missing || missing.length === 0
      ? "缺少合成材料"
      : `缺少合成材料: ${missing
          .slice(0, 4)
          .map((m) => `${m.item} x${m.shortfall}`)
          .join(", ")}`;
  if (woodInfo && woodInfo.wood_shortfall_logs > 0) {
    return `${base}（还需约 ${woodInfo.wood_shortfall_logs} 根原木）`;
  }
  return base;
}

export async function executeCraftAction(
  bot,
  params,
  { gotoFn = null, autoEquip = true, signal = null } = {},
) {
  const parsed = parseCraftParams(params);
  if (parsed.error) {
    return {
      ok: false,
      accepted: false,
      reason: parsed.error,
      summary: parsed.error === "invalid_item_name" ? "合成物品ID无效" : "合成数量需为1到16的整数",
    };
  }

  const { itemName, count: requestedCount } = parsed;
  const targetItem = bot?.registry?.itemsByName?.[itemName];
  if (!targetItem) {
    return {
      ok: false,
      accepted: true,
      reason: "unknown_item",
      item_name: itemName,
      requested_count: requestedCount,
      crafted_count: 0,
      missing_materials: [],
      summary: `未知物品: ${itemName}`,
    };
  }

  if (typeof bot.craft !== "function" || typeof bot.recipesFor !== "function") {
    return {
      ok: false,
      accepted: true,
      reason: "craft_failed",
      item_name: itemName,
      requested_count: requestedCount,
      crafted_count: 0,
      missing_materials: [],
      summary: "当前连接不支持合成",
    };
  }

  const tableBlockId = bot.registry?.blocksByName?.crafting_table?.id ?? 58;
  const dummyTableHint = { name: "crafting_table", type: tableBlockId };

  const handAllRecipes = getAllRecipesSafe(bot, targetItem.id, null);
  const allRecipesWithTable = getAllRecipesSafe(bot, targetItem.id, dummyTableHint);
  const allKnownRecipes = allRecipesWithTable.length > 0 ? allRecipesWithTable : handAllRecipes;

  if (allKnownRecipes.length === 0) {
    return {
      ok: false,
      accepted: true,
      reason: "no_recipe",
      item_name: itemName,
      requested_count: requestedCount,
      crafted_count: 0,
      missing_materials: [],
      summary: `无可用合成配方: ${itemName}`,
    };
  }

  const has2x2Recipe = handAllRecipes.length > 0;
  const nearbyTableBefore = has2x2Recipe ? null : findNearbyCraftingTable(bot, CRAFTING_TABLE_SEARCH_RADIUS);
  const countsInitial = countInventoryByName(bot);
  const hasTableInInventory = (countsInitial.get("crafting_table") ?? 0) > 0;
  const needsTableCraft = !has2x2Recipe && !nearbyTableBefore && !hasTableInInventory;

  // Step 1: Preprocess wood (<= 2 levels: log -> planks -> stick) if needed
  const directHandNow = getCraftableRecipesSafe(bot, targetItem.id, null);
  if (directHandNow.length === 0 || (requestedCount > 1 && has2x2Recipe) || !has2x2Recipe) {
    await preprocessWoodMaterials(bot, {
      targetItemName: itemName,
      candidateRecipes: has2x2Recipe ? handAllRecipes : allKnownRecipes,
      targetCount: requestedCount,
      needsTableCraft,
      signal,
    });
  }
  if (signal?.aborted) {
    return { aborted: true };
  }

  // Step 2: Try 2x2 hand craft first, otherwise prepare 3x3 crafting table
  let craftingTableBlock = null;
  let usedTable = false;

  const craftableHand = getCraftableRecipesSafe(bot, targetItem.id, null);
  if (craftableHand.length === 0 && !has2x2Recipe) {
    // Check if 3x3 recipe would even be craftable with a table before placing one
    const hypothetical3x3 = getCraftableRecipesSafe(bot, targetItem.id, dummyTableHint);
    if (hypothetical3x3.length === 0) {
      const countsNow = countInventoryByName(bot);
      const bestMiss = pickBestRecipeAnalysis(allKnownRecipes, bot, requestedCount, countsNow);
      const missing = bestMiss?.missing ?? [];
      let recipePlanksForOne = 0;
      if (bestMiss) {
        for (const [ingName, qty] of bestMiss.reqs.entries()) {
          if (isPlanksName(ingName)) recipePlanksForOne += qty;
        }
      }
      const woodInfo = computeWoodShortfall(missing, countsNow, {
        needsTableCraft,
        recipePlanksForOne,
      });
      return {
        ok: false,
        accepted: true,
        reason: "missing_materials",
        item_name: itemName,
        requested_count: requestedCount,
        crafted_count: 0,
        missing_materials: missing,
        ...(woodInfo.wood_shortfall_logs > 0 ? woodInfo : {}),
        summary: formatMissingSummary(missing, woodInfo),
      };
    }

    // Locate existing crafting_table within 16 blocks or place one within 2 blocks
    const nearby = findNearbyCraftingTable(bot, CRAFTING_TABLE_SEARCH_RADIUS);
    if (nearby) {
      if (nearby.dist > 3.5 && typeof gotoFn === "function") {
        await gotoFn(new GoalNear(nearby.pos.x, nearby.pos.y, nearby.pos.z, 2), signal);
        if (signal?.aborted) return { aborted: true };
      }
      craftingTableBlock = nearby.block;
      usedTable = true;
    } else {
      // Use crafting_table from inventory or craft one from >= 4 planks
      let curCounts = countInventoryByName(bot);
      if ((curCounts.get("crafting_table") ?? 0) === 0) {
        const tableItem = bot.registry?.itemsByName?.crafting_table;
        const tableRecipes = tableItem ? getCraftableRecipesSafe(bot, tableItem.id, null) : [];
        const bestTarget3x3 = pickBestRecipeAnalysis(allKnownRecipes, bot, 1, curCounts);
        let recipePlanksForOne = 0;
        if (bestTarget3x3) {
          for (const [ingName, qty] of bestTarget3x3.reqs.entries()) {
            if (isPlanksName(ingName)) recipePlanksForOne += qty;
          }
        }
        const totalPlanksNow = countAllPlanks(curCounts);
        if (tableRecipes.length > 0 && totalPlanksNow >= 4 + recipePlanksForOne) {
          try {
            await bot.craft(tableRecipes[0], 1, null);
          } catch {
            // Fall through to check below
          }
        }
        curCounts = countInventoryByName(bot);
      }

      if ((curCounts.get("crafting_table") ?? 0) === 0) {
        const havePlanks = countAllPlanks(curCounts);
        const bestTarget3x3 = pickBestRecipeAnalysis(allKnownRecipes, bot, 1, curCounts);
        let recipePlanksForOne = 0;
        if (bestTarget3x3) {
          for (const [ingName, qty] of bestTarget3x3.reqs.entries()) {
            if (isPlanksName(ingName)) recipePlanksForOne += qty;
          }
        }
        const needPlanks = 4 + recipePlanksForOne;
        const missing =
          havePlanks < needPlanks
            ? [
                {
                  item: "oak_planks",
                  need: needPlanks,
                  have: havePlanks,
                  shortfall: needPlanks - havePlanks,
                },
              ]
            : [{ item: "crafting_table", need: 1, have: 0, shortfall: 1 }];
        const woodInfo = computeWoodShortfall(missing, curCounts, {
          needsTableCraft: true,
          recipePlanksForOne,
        });
        return {
          ok: false,
          accepted: true,
          reason: "no_crafting_table",
          item_name: itemName,
          requested_count: requestedCount,
          crafted_count: 0,
          missing_materials: missing,
          ...(woodInfo.wood_shortfall_logs > 0 ? woodInfo : {}),
          summary:
            woodInfo.wood_shortfall_logs > 0
              ? `附近16格内无工作台且木料不足（还需约 ${woodInfo.wood_shortfall_logs} 根原木）`
              : "附近16格内无工作台且材料不足以制作工作台",
        };
      }

      const placed = await placeCraftingTableSafely(bot);
      if (!placed.ok) {
        return {
          ok: false,
          accepted: true,
          reason: placed.reason,
          item_name: itemName,
          requested_count: requestedCount,
          crafted_count: 0,
          missing_materials: [],
          summary: "周围2格内未找到安全实心地面放置工作台",
        };
      }
      craftingTableBlock = placed.tableBlock;
      usedTable = true;
    }
  }

  // Step 3: Craft batches up to requestedCount
  let craftedCount = 0;
  let craftError = false;
  while (craftedCount < requestedCount && !signal?.aborted) {
    const activeTable = usedTable ? craftingTableBlock : null;
    const availableRecipes = getCraftableRecipesSafe(bot, targetItem.id, activeTable);
    if (availableRecipes.length === 0) break;

    const recipe = availableRecipes[0];
    const perBatch = getRecipeResultCount(recipe);
    const remaining = requestedCount - craftedCount;
    const batchesDesired = Math.ceil(remaining / perBatch);
    const curCounts = countInventoryByName(bot);
    const reqs = extractRecipeIngredients(recipe, bot);

    let maxBatches = batchesDesired;
    if (reqs.size > 0) {
      for (const [ingName, qty] of reqs.entries()) {
        const have = curCounts.get(ingName) ?? 0;
        maxBatches = Math.min(maxBatches, Math.floor(have / qty));
      }
    }
    if (maxBatches <= 0) {
      // Fallback: try 1 batch if recipe didn't expose delta/ingredients
      if (reqs.size === 0) {
        maxBatches = 1;
      } else {
        break;
      }
    }

    try {
      await bot.craft(recipe, maxBatches, activeTable);
      craftedCount += maxBatches * perBatch;
    } catch {
      craftError = true;
      break;
    }
  }

  if (signal?.aborted) {
    return { aborted: true };
  }

  if (craftedCount === 0) {
    const countsAfter = countInventoryByName(bot);
    const bestMiss = pickBestRecipeAnalysis(allKnownRecipes, bot, requestedCount, countsAfter);
    const missing = bestMiss?.missing ?? [];
    if (missing.length > 0) {
      const woodInfo = computeWoodShortfall(missing, countsAfter, {
        needsTableCraft: !usedTable && needsTableCraft,
      });
      return {
        ok: false,
        accepted: true,
        reason: "missing_materials",
        item_name: itemName,
        requested_count: requestedCount,
        crafted_count: 0,
        missing_materials: missing,
        ...(woodInfo.wood_shortfall_logs > 0 ? woodInfo : {}),
        summary: formatMissingSummary(missing, woodInfo),
      };
    }
    return {
      ok: false,
      accepted: true,
      reason: craftError ? "craft_failed" : "missing_materials",
      item_name: itemName,
      requested_count: requestedCount,
      crafted_count: 0,
      missing_materials: [],
      summary: craftError ? `合成${itemName}失败` : `缺少合成${itemName}的材料`,
    };
  }

  // Step 4: Post-craft auto-equip check
  const equippedItems = [];
  if (autoEquip) {
    const eqGear = await autoEquipBestGear(bot, { includeShield: true });
    equippedItems.push(...eqGear);
    if (getWeaponScore(itemName) > 0) {
      const prevHeld = extractCanonicalItemName(bot.heldItem);
      const bestWeapon = await equipBestWeapon(bot);
      if (bestWeapon && bestWeapon !== prevHeld && !equippedItems.includes(bestWeapon)) {
        equippedItems.push(bestWeapon);
      }
    }
  }

  const partial = craftedCount < requestedCount;
  let remainingMissing = [];
  if (partial) {
    const countsEnd = countInventoryByName(bot);
    const bestRem = pickBestRecipeAnalysis(
      allKnownRecipes,
      bot,
      requestedCount - craftedCount,
      countsEnd,
    );
    remainingMissing = bestRem?.missing ?? [];
  }

  const summary = partial
    ? `已部分合成 ${itemName} x${craftedCount}/${requestedCount}，${formatMissingSummary(remainingMissing)}`
    : `已合成 ${itemName} x${craftedCount}`;

  return {
    ok: true,
    accepted: true,
    item_name: itemName,
    requested_count: requestedCount,
    crafted_count: craftedCount,
    partial,
    used_table: usedTable,
    equipped_items: equippedItems,
    missing_materials: remainingMissing,
    summary,
  };
}
