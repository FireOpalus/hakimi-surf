import { normalizeCanonicalItemId } from "./protocol.mjs";
import {
  collectEquipmentSnapshot,
  detectEnvironmentalHazard,
} from "./survival.mjs";
import {
  CRAFTING_TABLE_SEARCH_RADIUS,
  countInventoryByName,
  executeCraftAction,
  findNearbyCraftingTable,
  getPlanksForLog,
  isPlanksName,
} from "./crafting.mjs";
import {
  FURNACE_SEARCH_RADIUS,
  executeSmeltAction,
  findNearbyFurnace,
  pickBestFuel,
} from "./smelting.mjs";
import {
  checkHarvestToolTier,
  executeHuntAction,
  isBedName,
} from "./world_actions.mjs";
import { CREEPER_SAFE_DISTANCE, findNearestAnyHostile, findNearestCreeper } from "./combat.mjs";

export const MIN_ACQUIRE_COUNT = 1;
export const MAX_ACQUIRE_COUNT = 16;
export const MAX_ACQUIRE_DEPTH = 6;
export const MAX_ACQUIRE_STEPS = 12;

export const OVERWORLD_LOG_BLOCKS = Object.freeze([
  "oak_log",
  "birch_log",
  "spruce_log",
  "jungle_log",
  "acacia_log",
  "dark_oak_log",
  "mangrove_log",
  "cherry_log",
  "pale_oak_log",
]);

const LOG_SPECIES_SET = new Set(OVERWORLD_LOG_BLOCKS);

const WOOL_COLORS = Object.freeze([
  "white",
  "light_gray",
  "gray",
  "black",
  "brown",
  "red",
  "orange",
  "yellow",
  "lime",
  "green",
  "cyan",
  "light_blue",
  "blue",
  "purple",
  "magenta",
  "pink",
]);

const BED_NAMES = new Set(WOOL_COLORS.map((c) => `${c}_bed`));

export const ACQUIRE_CRAFT_RECIPES = Object.freeze({
  crafting_table: Object.freeze({ planks: 4, needsTable: false, yield: 1 }),
  stick: Object.freeze({ planks: 2, needsTable: false, yield: 4 }),
  chest: Object.freeze({ planks: 8, needsTable: true, yield: 1 }),
  wooden_pickaxe: Object.freeze({ planks: 3, stick: 2, needsTable: true, yield: 1, sameSpeciesPlanks: true }),
  wooden_sword: Object.freeze({ planks: 2, stick: 1, needsTable: true, yield: 1, sameSpeciesPlanks: true }),
  wooden_axe: Object.freeze({ planks: 3, stick: 2, needsTable: true, yield: 1, sameSpeciesPlanks: true }),
  wooden_shovel: Object.freeze({ planks: 1, stick: 2, needsTable: true, yield: 1, sameSpeciesPlanks: true }),
  stone_pickaxe: Object.freeze({ cobblestone: 3, stick: 2, needsTable: true, yield: 1 }),
  stone_sword: Object.freeze({ cobblestone: 2, stick: 1, needsTable: true, yield: 1 }),
  stone_axe: Object.freeze({ cobblestone: 3, stick: 2, needsTable: true, yield: 1 }),
  stone_shovel: Object.freeze({ cobblestone: 1, stick: 2, needsTable: true, yield: 1 }),
  furnace: Object.freeze({ cobblestone: 8, needsTable: true, yield: 1 }),
  torch: Object.freeze({ fuel_coal: 1, stick: 1, needsTable: false, yield: 4 }),
  shield: Object.freeze({ planks: 6, iron_ingot: 1, needsTable: true, yield: 1, sameSpeciesPlanks: true }),
  iron_pickaxe: Object.freeze({ iron_ingot: 3, stick: 2, needsTable: true, yield: 1 }),
  iron_sword: Object.freeze({ iron_ingot: 2, stick: 1, needsTable: true, yield: 1 }),
  iron_axe: Object.freeze({ iron_ingot: 3, stick: 2, needsTable: true, yield: 1 }),
  iron_shovel: Object.freeze({ iron_ingot: 1, stick: 2, needsTable: true, yield: 1 }),
  iron_helmet: Object.freeze({ iron_ingot: 5, needsTable: true, yield: 1 }),
  iron_chestplate: Object.freeze({ iron_ingot: 8, needsTable: true, yield: 1 }),
  iron_leggings: Object.freeze({ iron_ingot: 7, needsTable: true, yield: 1 }),
  iron_boots: Object.freeze({ iron_ingot: 4, needsTable: true, yield: 1 }),
  bucket: Object.freeze({ iron_ingot: 3, needsTable: true, yield: 1 }),
  bread: Object.freeze({ wheat: 3, needsTable: true, yield: 1 }),
});

export const ACQUIRE_SMELT_SOURCES = Object.freeze({
  iron_ingot: Object.freeze({ input: "raw_iron", gatherBlocks: ["iron_ore", "deepslate_iron_ore"], minPickaxe: "stone_pickaxe" }),
  charcoal: Object.freeze({ input: "any_log", gatherBlocks: OVERWORLD_LOG_BLOCKS, minPickaxe: "" }),
  cooked_beef: Object.freeze({ input: "beef", huntMob: "cow" }),
  cooked_porkchop: Object.freeze({ input: "porkchop", huntMob: "pig" }),
  cooked_mutton: Object.freeze({ input: "mutton", huntMob: "sheep" }),
  cooked_chicken: Object.freeze({ input: "chicken", huntMob: "chicken" }),
  cooked_rabbit: Object.freeze({ input: "rabbit", huntMob: "rabbit" }),
});

export const ACQUIRE_ALLOWED_ITEMS = new Set([
  ...Object.keys(ACQUIRE_CRAFT_RECIPES),
  ...Object.keys(ACQUIRE_SMELT_SOURCES),
  "bed",
  ...BED_NAMES,
  "cobblestone",
  "coal",
  "raw_iron",
  "wheat",
  "oak_planks",
  "birch_planks",
  "spruce_planks",
  "jungle_planks",
  "acacia_planks",
  "dark_oak_planks",
  "mangrove_planks",
  "cherry_planks",
  "pale_oak_planks",
]);

export function parseAcquireParams(params) {
  const rawItem = params?.item_name ?? params?.item;
  if (typeof rawItem !== "string") {
    return { error: "invalid_item_name" };
  }
  const itemName = normalizeCanonicalItemId(rawItem);
  if (!itemName) {
    return { error: "invalid_item_name" };
  }
  if (!ACQUIRE_ALLOWED_ITEMS.has(itemName)) {
    return { error: "acquire_item_not_supported", itemName };
  }
  const rawCount = params?.count === undefined ? 1 : params.count;
  if (
    typeof rawCount !== "number" ||
    !Number.isInteger(rawCount) ||
    rawCount < MIN_ACQUIRE_COUNT ||
    rawCount > MAX_ACQUIRE_COUNT
  ) {
    return { error: "invalid_acquire_count" };
  }
  return { itemName, count: rawCount };
}

function countOwnedItem(bot, itemName) {
  const counts = countInventoryByName(bot);
  if (itemName === "bed") {
    let total = 0;
    for (const [name, cnt] of counts.entries()) {
      if (isBedName(name)) total += cnt;
    }
    return total;
  }
  let have = counts.get(itemName) ?? 0;
  // Equipment slots (e.g. off-hand shield or worn armor) may not be in inventory.items() on some mocks
  const eq = collectEquipmentSnapshot(bot);
  const eqCount = Object.values(eq).filter((n) => n === itemName).length;
  if (have === 0 && eqCount > 0) {
    have = eqCount;
  }
  return have;
}

function resolveBedTarget(bot, requested) {
  if (requested !== "bed") return requested;
  const counts = countInventoryByName(bot);
  let bestColor = "white";
  let bestQty = 0;
  for (const color of WOOL_COLORS) {
    const qty = counts.get(`${color}_wool`) ?? 0;
    if (qty > bestQty) {
      bestQty = qty;
      bestColor = color;
    }
  }
  return `${bestColor}_bed`;
}

function computeWoodInventory(counts) {
  let totalPlanks = 0;
  let totalLogPlanks = 0;
  const bySpecies = new Map();

  for (const [name, cnt] of counts.entries()) {
    if (cnt <= 0) continue;
    if (isPlanksName(name)) {
      totalPlanks += cnt;
      const cur = bySpecies.get(name) || { planks: 0, logs: 0, logName: "" };
      cur.planks += cnt;
      bySpecies.set(name, cur);
    } else {
      const plankName = getPlanksForLog(name);
      if (plankName) {
        const yieldPerLog = name.includes("bamboo_block") ? 2 : 4;
        totalLogPlanks += cnt * yieldPerLog;
        const cur = bySpecies.get(plankName) || { planks: 0, logs: 0, logName: "" };
        cur.logs += cnt;
        if (!cur.logName && LOG_SPECIES_SET.has(name)) {
          cur.logName = name;
        }
        bySpecies.set(plankName, cur);
      }
    }
  }

  let bestSpeciesPlankName = "";
  let bestSpeciesPotential = 0;
  let bestSpeciesLogName = "";
  for (const [plankName, info] of bySpecies.entries()) {
    const pot = info.planks + info.logs * 4;
    if (pot > bestSpeciesPotential) {
      bestSpeciesPotential = pot;
      bestSpeciesPlankName = plankName;
      bestSpeciesLogName =
        info.logName ||
        (plankName.endsWith("_planks") ? `${plankName.slice(0, -"_planks".length)}_log` : "");
    }
  }

  return {
    totalPlanks,
    totalLogPlanks,
    totalPotentialPlanks: totalPlanks + totalLogPlanks,
    bestSpeciesPlankName,
    bestSpeciesPotential,
    bestSpeciesLogName,
  };
}

function findAvailableLogBlockOrder(bot, preferredLogName = "") {
  const ordered = [];
  if (preferredLogName && LOG_SPECIES_SET.has(preferredLogName)) {
    ordered.push(preferredLogName);
  }
  if (typeof bot?.findBlocks === "function" && bot.entity?.position) {
    try {
      const positions =
        bot.findBlocks({
          matching: (b) => Boolean(b && LOG_SPECIES_SET.has(b.name)),
          point: bot.entity.position,
          maxDistance: 32,
          count: 16,
        }) || [];
      for (const p of positions) {
        const blk = typeof bot.blockAt === "function" ? bot.blockAt(p) : null;
        if (blk && LOG_SPECIES_SET.has(blk.name) && !ordered.includes(blk.name)) {
          ordered.push(blk.name);
        }
      }
    } catch {
      // Fall back to default log list
    }
  }
  for (const logName of OVERWORLD_LOG_BLOCKS) {
    if (!ordered.includes(logName)) ordered.push(logName);
  }
  return ordered;
}

function hasAnyNearbyBlock(bot, blockNames, maxDistance = 32) {
  if (!bot?.entity?.position || typeof bot.findBlocks !== "function") return false;
  const set = new Set(blockNames);
  try {
    const hits =
      bot.findBlocks({
        matching: (b) => Boolean(b && set.has(b.name)),
        point: bot.entity.position,
        maxDistance,
        count: 1,
      }) || [];
    return hits.length > 0;
  } catch {
    return false;
  }
}

function findAnyLogInInventory(counts) {
  let best = "";
  let bestCount = 0;
  for (const [name, cnt] of counts.entries()) {
    if (LOG_SPECIES_SET.has(name) && cnt > bestCount) {
      best = name;
      bestCount = cnt;
    }
  }
  return { logName: best, count: bestCount };
}

/**
 * AltoClef-inspired 0-Token bounded recursive resource & crafting solver.
 * Ensures the bot owns at least `count` of `item_name` by decomposing missing
 * wood, stone, coal, raw iron, furnace, smelting, and crafting steps locally.
 */
export async function executeAcquireAction(
  bot,
  params,
  {
    gatherBlocksFn,
    gotoFn = null,
    collectDropsFn = null,
    autoEquip = true,
    combatMode = "evade",
    signal = null,
    nowFn = () => Date.now(),
    deadlineMs = null,
    maxSteps = MAX_ACQUIRE_STEPS,
    maxDepth = MAX_ACQUIRE_DEPTH,
    smeltPollIntervalMs = 350,
    huntAttackCooldownMs = null,
  } = {},
) {
  const parsed = parseAcquireParams(params);
  if (parsed.error) {
    return {
      ok: false,
      accepted: false,
      reason: parsed.error,
      item_name: parsed.itemName || "",
      summary:
        parsed.error === "acquire_item_not_supported"
          ? `暂不支持自动筹料合成: ${parsed.itemName}`
          : parsed.error === "invalid_item_name"
            ? "目标物品ID无效"
            : "筹料数量需为1到16的整数",
    };
  }

  const requestedItem = parsed.itemName;
  const requestedCount = parsed.count;
  const effectiveTarget = resolveBedTarget(bot, requestedItem);
  const initialHave = countOwnedItem(bot, requestedItem);

  if (initialHave >= requestedCount) {
    return {
      ok: true,
      accepted: true,
      item_name: effectiveTarget,
      requested_count: requestedCount,
      acquired_count: 0,
      owned_count: initialHave,
      already_owned: true,
      steps_completed: [],
      summary: `背包已有 ${effectiveTarget} x${initialHave}，无需重复筹料`,
    };
  }

  const ctx = {
    steps: [],
    stepCount: 0,
    maxSteps: Math.max(1, Math.min(20, maxSteps)),
    maxDepth: Math.max(1, Math.min(8, maxDepth)),
    deadlineMs:
      typeof deadlineMs === "number" && Number.isFinite(deadlineMs)
        ? deadlineMs
        : nowFn() + 28_000,
    visiting: new Set(),
    blockerReason: "",
    blockerItem: "",
  };

  const checkInterrupt = () => {
    if (signal?.aborted) {
      return { stopped: true, reason: "mc_goal_cancelled" };
    }
    if (nowFn() >= ctx.deadlineMs) {
      return { stopped: true, reason: "time_budget_reached" };
    }
    if (ctx.stepCount >= ctx.maxSteps) {
      return { stopped: true, reason: "step_limit_reached" };
    }
    const hazard = detectEnvironmentalHazard(bot);
    if (hazard.severity >= 2) {
      return { stopped: true, reason: "interrupted_by_hazard" };
    }
    if (findNearestCreeper(bot, CREEPER_SAFE_DISTANCE)) {
      return { stopped: true, reason: "interrupted_by_threat" };
    }
    if (
      combatMode === "evade" &&
      typeof bot.health === "number" &&
      bot.health <= 7 &&
      findNearestAnyHostile(bot, 12)
    ) {
      return { stopped: true, reason: "interrupted_by_low_health" };
    }
    return null;
  };

  const recordBlocker = (reason, item) => {
    if (!ctx.blockerReason) {
      ctx.blockerReason = reason;
      ctx.blockerItem = item || "";
    }
  };

  const runGather = async (blockNames, count, labelItem) => {
    const intr = checkInterrupt();
    if (intr) return { ok: false, reason: intr.reason, item: labelItem };
    if (typeof gatherBlocksFn !== "function") {
      return { ok: false, reason: "gather_unavailable", item: labelItem };
    }
    ctx.stepCount += 1;
    const boundedCount = Math.max(1, Math.min(8, Math.ceil(count)));
    const res = await gatherBlocksFn(blockNames, boundedCount, signal);
    if (res?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: labelItem };
    if (res?.ok && (res.collected_count ?? 0) > 0) {
      const blk = res.block || blockNames[0] || labelItem;
      ctx.steps.push(`gather:${blk}x${res.collected_count}`);
      return { ok: true, collected: res.collected_count, block: blk };
    }
    return {
      ok: false,
      reason: res?.reason || "block_not_found",
      item: labelItem || blockNames[0] || "",
    };
  };

  const ensureWoodPlanks = async (
    totalPlanksRequired,
    { sameSpeciesMin = 0, preferredSpeciesPlanks = "" } = {},
    depth = 0,
  ) => {
    const intr = checkInterrupt();
    if (intr) return { ok: false, reason: intr.reason, item: "log" };
    const counts = countInventoryByName(bot);
    const wood = computeWoodInventory(counts);
    const totalShortfall = Math.max(0, totalPlanksRequired - wood.totalPotentialPlanks);
    const speciesShortfall =
      sameSpeciesMin > 0
        ? Math.max(0, sameSpeciesMin - wood.bestSpeciesPotential)
        : 0;
    const planksNeeded = Math.max(totalShortfall, speciesShortfall);
    if (planksNeeded <= 0) {
      return { ok: true };
    }
    const logsNeeded = Math.max(1, Math.min(6, Math.ceil(planksNeeded / 4)));
    let preferredLog = "";
    if (preferredSpeciesPlanks && preferredSpeciesPlanks.endsWith("_planks")) {
      preferredLog = `${preferredSpeciesPlanks.slice(0, -"_planks".length)}_log`;
    } else if (wood.bestSpeciesLogName) {
      preferredLog = wood.bestSpeciesLogName;
    }
    const logOrder = findAvailableLogBlockOrder(bot, preferredLog);
    // Try gathering a single consistent species first to avoid split-species planks.
    for (const singleLog of logOrder.slice(0, 3)) {
      if (!hasAnyNearbyBlock(bot, [singleLog], 32)) continue;
      const g = await runGather([singleLog], logsNeeded, singleLog);
      if (g.ok) {
        const afterWood = computeWoodInventory(countInventoryByName(bot));
        if (
          afterWood.totalPotentialPlanks >= totalPlanksRequired &&
          (sameSpeciesMin <= 0 || afterWood.bestSpeciesPotential >= sameSpeciesMin)
        ) {
          return { ok: true };
        }
        if (depth < ctx.maxDepth - 1) {
          return await ensureWoodPlanks(
            totalPlanksRequired,
            { sameSpeciesMin, preferredSpeciesPlanks },
            depth + 1,
          );
        }
      }
      if (g.reason === "mc_goal_cancelled" || g.reason.startsWith("interrupted_")) {
        return g;
      }
    }
    // Fallback: gather any overworld log
    const fallback = await runGather(OVERWORLD_LOG_BLOCKS, logsNeeded, "oak_log");
    if (!fallback.ok) {
      recordBlocker(fallback.reason, fallback.item || "oak_log");
      return fallback;
    }
    const afterWood = computeWoodInventory(countInventoryByName(bot));
    if (
      afterWood.totalPotentialPlanks >= totalPlanksRequired &&
      (sameSpeciesMin <= 0 || afterWood.bestSpeciesPotential >= sameSpeciesMin)
    ) {
      return { ok: true };
    }
    recordBlocker("missing_materials", "oak_log");
    return { ok: false, reason: "missing_materials", item: "oak_log" };
  };

  const ensureFuelForSmelt = async (smeltCount, depth) => {
    const counts = countInventoryByName(bot);
    if (pickBestFuel(counts, { targetCount: smeltCount })) {
      return { ok: true };
    }
    if (hasAnyNearbyBlock(bot, ["coal_ore", "deepslate_coal_ore"], 32)) {
      const coalRes = await ensureItem("coal", 1, depth + 1);
      if (coalRes.ok) return { ok: true };
    }
    // Fall back to 2 logs -> planks as fuel
    const woodRes = await ensureWoodPlanks(4, {}, depth + 1);
    return woodRes;
  };

  const ensureItem = async (rawTarget, desiredCount, depth = 0) => {
    const intr = checkInterrupt();
    if (intr) return { ok: false, reason: intr.reason, item: rawTarget };
    if (depth > ctx.maxDepth) {
      recordBlocker("max_depth_reached", rawTarget);
      return { ok: false, reason: "max_depth_reached", item: rawTarget };
    }

    const target = resolveBedTarget(bot, rawTarget);
    const haveNow = countOwnedItem(bot, rawTarget === "bed" ? "bed" : target);
    if (haveNow >= desiredCount) {
      return { ok: true };
    }
    const need = desiredCount - haveNow;

    if (ctx.visiting.has(target)) {
      recordBlocker("dependency_cycle", target);
      return { ok: false, reason: "dependency_cycle", item: target };
    }
    ctx.visiting.add(target);

    try {
      // 1. Direct gatherable materials
      if (target === "cobblestone") {
        const tier = checkHarvestToolTier(bot, "stone");
        if (!tier.ok) {
          const pickRes = await ensureItem(tier.requiredTool || "wooden_pickaxe", 1, depth + 1);
          if (!pickRes.ok) return pickRes;
        }
        const g = await runGather(["stone", "cobblestone"], need, "cobblestone");
        if (!g.ok) {
          recordBlocker(g.reason, "stone");
          return g;
        }
        const after = countOwnedItem(bot, "cobblestone");
        if (after >= desiredCount) return { ok: true };
        recordBlocker("partial_gather", "stone");
        return { ok: false, reason: "partial_gather", item: "stone" };
      }

      if (target === "coal") {
        const counts = countInventoryByName(bot);
        if ((counts.get("coal") ?? 0) + (counts.get("charcoal") ?? 0) >= desiredCount) {
          return { ok: true };
        }
        if (hasAnyNearbyBlock(bot, ["coal_ore", "deepslate_coal_ore"], 32)) {
          const tier = checkHarvestToolTier(bot, "coal_ore");
          if (!tier.ok) {
            const pickRes = await ensureItem(tier.requiredTool || "wooden_pickaxe", 1, depth + 1);
            if (!pickRes.ok) return pickRes;
          }
          const g = await runGather(["coal_ore", "deepslate_coal_ore"], need, "coal_ore");
          if (g.ok) return { ok: true };
        }
        // Fallback: smelt charcoal from logs
        return await ensureItem("charcoal", desiredCount, depth + 1);
      }

      if (target === "raw_iron") {
        const tier = checkHarvestToolTier(bot, "iron_ore");
        if (!tier.ok) {
          const pickRes = await ensureItem(tier.requiredTool || "stone_pickaxe", 1, depth + 1);
          if (!pickRes.ok) return pickRes;
        }
        const g = await runGather(["iron_ore", "deepslate_iron_ore"], need, "iron_ore");
        if (!g.ok) {
          recordBlocker(g.reason, "iron_ore");
          return g;
        }
        const after = countOwnedItem(bot, "raw_iron");
        if (after >= desiredCount) return { ok: true };
        recordBlocker("partial_gather", "iron_ore");
        return { ok: false, reason: "partial_gather", item: "iron_ore" };
      }

      if (target === "wheat") {
        const counts = countInventoryByName(bot);
        if ((counts.get("hay_block") ?? 0) > 0) {
          ctx.stepCount += 1;
          const unpack = await executeCraftAction(
            bot,
            { item_name: "wheat", count: Math.min(16, need) },
            { gotoFn, autoEquip: false, signal },
          );
          if (unpack?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: "wheat" };
          if (unpack?.ok) {
            ctx.steps.push(`craft:wheatx${unpack.crafted_count}`);
            if (countOwnedItem(bot, "wheat") >= desiredCount) return { ok: true };
          }
        }
        const g = await runGather(["hay_block", "wheat"], Math.min(4, need), "wheat");
        if (!g.ok) {
          recordBlocker(g.reason, "wheat");
          return g;
        }
        const afterCounts = countInventoryByName(bot);
        if ((afterCounts.get("hay_block") ?? 0) > 0 && countOwnedItem(bot, "wheat") < desiredCount) {
          ctx.stepCount += 1;
          const unpack = await executeCraftAction(
            bot,
            { item_name: "wheat", count: Math.min(16, desiredCount - countOwnedItem(bot, "wheat")) },
            { gotoFn, autoEquip: false, signal },
          );
          if (unpack?.ok) ctx.steps.push(`craft:wheatx${unpack.crafted_count}`);
        }
        if (countOwnedItem(bot, "wheat") >= desiredCount) return { ok: true };
        recordBlocker("missing_materials", "wheat");
        return { ok: false, reason: "missing_materials", item: "wheat" };
      }

      if (isPlanksName(target)) {
        const w = await ensureWoodPlanks(
          desiredCount,
          { sameSpeciesMin: desiredCount, preferredSpeciesPlanks: target },
          depth + 1,
        );
        if (!w.ok) return w;
        ctx.stepCount += 1;
        const c = await executeCraftAction(
          bot,
          { item_name: target, count: Math.min(16, need) },
          { gotoFn, autoEquip: false, signal },
        );
        if (c?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: target };
        if (c?.ok) {
          ctx.steps.push(`craft:${target}x${c.crafted_count}`);
          return { ok: true };
        }
        recordBlocker(c?.reason || "craft_failed", target);
        return { ok: false, reason: c?.reason || "craft_failed", item: target };
      }

      // 2. Smelted targets (iron_ingot, charcoal, cooked meats)
      if (Object.hasOwn(ACQUIRE_SMELT_SOURCES, target)) {
        const spec = ACQUIRE_SMELT_SOURCES[target];
        let smeltInput = spec.input;
        if (smeltInput === "raw_iron") {
          const rawRes = await ensureItem("raw_iron", need, depth + 1);
          if (!rawRes.ok) return rawRes;
        } else if (smeltInput === "any_log") {
          const logCheck = findAnyLogInInventory(countInventoryByName(bot));
          if (logCheck.count < need + 1) {
            const g = await runGather(OVERWORLD_LOG_BLOCKS, need + 1 - logCheck.count, "oak_log");
            if (!g.ok) {
              recordBlocker(g.reason, "oak_log");
              return g;
            }
          }
          const afterLog = findAnyLogInInventory(countInventoryByName(bot));
          if (!afterLog.logName) {
            recordBlocker("missing_materials", "oak_log");
            return { ok: false, reason: "missing_materials", item: "oak_log" };
          }
          smeltInput = afterLog.logName;
        } else if (spec.huntMob) {
          const curRaw = countOwnedItem(bot, smeltInput);
          if (curRaw < need) {
            const intrHunt = checkInterrupt();
            if (intrHunt) return { ok: false, reason: intrHunt.reason, item: smeltInput };
            ctx.stepCount += 1;
            const huntRes = await executeHuntAction(
              bot,
              { target_mob: spec.huntMob, count: Math.min(3, need - curRaw) },
              {
                signal,
                gotoGoal: gotoFn,
                collectDrops: collectDropsFn,
                attackCooldownOverrideMs: huntAttackCooldownMs,
              },
            );
            if (huntRes?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: smeltInput };
            if (huntRes?.ok) {
              ctx.steps.push(`hunt:${spec.huntMob}x${huntRes.hunted_count ?? 1}`);
            }
            if (countOwnedItem(bot, smeltInput) <= 0) {
              recordBlocker(huntRes?.reason || "mob_not_found", spec.huntMob);
              return { ok: false, reason: huntRes?.reason || "mob_not_found", item: spec.huntMob };
            }
          }
        }

        // Ensure furnace exists nearby or in inventory
        const nearbyFurnace = findNearbyFurnace(bot, target === "iron_ingot" ? "ore" : "food", FURNACE_SEARCH_RADIUS);
        if (!nearbyFurnace && countOwnedItem(bot, "furnace") === 0) {
          const furnRes = await ensureItem("furnace", 1, depth + 1);
          if (!furnRes.ok) return furnRes;
        }

        // Ensure fuel exists
        const fuelRes = await ensureFuelForSmelt(need, depth + 1);
        if (!fuelRes.ok) return fuelRes;

        const intrSmelt = checkInterrupt();
        if (intrSmelt) return { ok: false, reason: intrSmelt.reason, item: target };

        const remainingMs = Math.max(500, ctx.deadlineMs - nowFn() - 1_000);
        ctx.stepCount += 1;
        const smeltRes = await executeSmeltAction(
          bot,
          { item_name: smeltInput, count: Math.min(16, need) },
          {
            gotoFn,
            signal,
            pollIntervalMs: smeltPollIntervalMs,
            maxSmeltWaitMs: remainingMs,
          },
        );
        if (smeltRes?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: target };
        if (smeltRes?.ok) {
          ctx.steps.push(`smelt:${target}x${smeltRes.smelted_count ?? 1}`);
          if (countOwnedItem(bot, target) >= desiredCount || (smeltRes.smelted_count ?? 0) >= need) {
            return { ok: true };
          }
        }
        recordBlocker(smeltRes?.reason || "smelt_failed", target);
        return { ok: false, reason: smeltRes?.reason || "smelt_failed", item: target };
      }

      // 3. Bed crafting (3 matching wool + 3 planks + table)
      if (isBedName(target)) {
        const color = target.slice(0, -"_bed".length);
        const woolName = `${color}_wool`;
        if (countOwnedItem(bot, woolName) < 3) {
          const intrWool = checkInterrupt();
          if (intrWool) return { ok: false, reason: intrWool.reason, item: woolName };
          ctx.stepCount += 1;
          const huntRes = await executeHuntAction(
            bot,
            { target_mob: "sheep", count: Math.min(3, 3 - countOwnedItem(bot, woolName)) },
            {
              signal,
              gotoGoal: gotoFn,
              collectDrops: collectDropsFn,
              attackCooldownOverrideMs: huntAttackCooldownMs,
            },
          );
          if (huntRes?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: woolName };
          if (huntRes?.ok) {
            ctx.steps.push(`hunt:sheepx${huntRes.hunted_count ?? 1}`);
          }
          if (countOwnedItem(bot, woolName) < 3) {
            recordBlocker("missing_materials", woolName);
            return { ok: false, reason: "missing_materials", item: woolName };
          }
        }
        const needTable =
          !findNearbyCraftingTable(bot, CRAFTING_TABLE_SEARCH_RADIUS) &&
          countOwnedItem(bot, "crafting_table") === 0;
        const woodRes = await ensureWoodPlanks(
          3 + (needTable ? 4 : 0),
          { sameSpeciesMin: 3 },
          depth + 1,
        );
        if (!woodRes.ok) return woodRes;

        ctx.stepCount += 1;
        const craftBed = await executeCraftAction(
          bot,
          { item_name: target, count: 1 },
          { gotoFn, autoEquip: false, signal },
        );
        if (craftBed?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: target };
        if (craftBed?.ok) {
          ctx.steps.push(`craft:${target}x${craftBed.crafted_count}`);
          return { ok: true };
        }
        recordBlocker(craftBed?.reason || "craft_failed", target);
        return { ok: false, reason: craftBed?.reason || "craft_failed", item: target };
      }

      // 4. Standard crafting recipe targets
      const recipe = Object.hasOwn(ACQUIRE_CRAFT_RECIPES, target)
        ? ACQUIRE_CRAFT_RECIPES[target]
        : null;
      if (!recipe) {
        recordBlocker("acquire_item_not_supported", target);
        return { ok: false, reason: "acquire_item_not_supported", item: target };
      }

      const batches = Math.max(1, Math.ceil(need / (recipe.yield || 1)));

      // Resolve non-wood dependencies first (cobblestone, iron_ingot, fuel_coal, wheat)
      if (recipe.cobblestone) {
        const cobRes = await ensureItem("cobblestone", recipe.cobblestone * batches, depth + 1);
        if (!cobRes.ok) return cobRes;
      }
      if (recipe.iron_ingot) {
        const ironRes = await ensureItem("iron_ingot", recipe.iron_ingot * batches, depth + 1);
        if (!ironRes.ok) return ironRes;
      }
      if (recipe.wheat) {
        const wheatRes = await ensureItem("wheat", recipe.wheat * batches, depth + 1);
        if (!wheatRes.ok) return wheatRes;
      }
      if (recipe.fuel_coal) {
        const counts = countInventoryByName(bot);
        const haveCoalLike = (counts.get("coal") ?? 0) + (counts.get("charcoal") ?? 0);
        if (haveCoalLike < recipe.fuel_coal * batches) {
          const coalRes = await ensureItem("coal", recipe.fuel_coal * batches, depth + 1);
          if (!coalRes.ok) return coalRes;
        }
      }

      // Resolve wood (planks + sticks + optional crafting_table planks) in a single pass
      const countsBeforeWood = countInventoryByName(bot);
      const haveSticks = countsBeforeWood.get("stick") ?? 0;
      const needSticks = (recipe.stick ?? 0) * batches;
      const stickGap = Math.max(0, needSticks - haveSticks);
      const planksForSticks = stickGap > 0 ? Math.ceil(stickGap / 4) * 2 : 0;
      const recipePlanks = (recipe.planks ?? 0) * batches;
      const needTableWood =
        Boolean(recipe.needsTable) &&
        !findNearbyCraftingTable(bot, CRAFTING_TABLE_SEARCH_RADIUS) &&
        (countsBeforeWood.get("crafting_table") ?? 0) === 0
          ? 4
          : 0;
      const totalPlanksNeed = recipePlanks + planksForSticks + needTableWood;
      if (totalPlanksNeed > 0) {
        const woodRes = await ensureWoodPlanks(
          totalPlanksNeed,
          { sameSpeciesMin: recipe.sameSpeciesPlanks ? recipePlanks : 0 },
          depth + 1,
        );
        if (!woodRes.ok) return woodRes;
      }

      const intrCraft = checkInterrupt();
      if (intrCraft) return { ok: false, reason: intrCraft.reason, item: target };

      ctx.stepCount += 1;
      const craftRes = await executeCraftAction(
        bot,
        { item_name: target, count: Math.min(16, need) },
        { gotoFn, autoEquip: autoEquip && depth === 0, signal },
      );
      if (craftRes?.aborted) return { ok: false, reason: "mc_goal_cancelled", item: target };
      if (craftRes?.ok) {
        ctx.steps.push(`craft:${target}x${craftRes.crafted_count}`);
        return {
          ok: true,
          crafted_count: craftRes.crafted_count,
          equipped_items: craftRes.equipped_items || [],
        };
      }
      recordBlocker(craftRes?.reason || "craft_failed", target);
      return { ok: false, reason: craftRes?.reason || "craft_failed", item: target };
    } finally {
      ctx.visiting.delete(target);
    }
  };

  const finalRes = await ensureItem(requestedItem, requestedCount, 0);
  const finalOwned = countOwnedItem(bot, requestedItem);
  const acquiredCount = Math.max(0, finalOwned - initialHave);

  if (finalRes.ok || finalOwned >= requestedCount) {
    const effectiveAcquired =
      acquiredCount > 0 ? acquiredCount : Math.max(1, finalRes.crafted_count ?? 1);
    const stepSummary = ctx.steps.length > 0 ? `（经 ${ctx.steps.slice(-4).join(" -> ")}）` : "";
    return {
      ok: true,
      accepted: true,
      item_name: effectiveTarget,
      requested_count: requestedCount,
      acquired_count: effectiveAcquired,
      owned_count: Math.max(finalOwned, effectiveAcquired),
      already_owned: false,
      steps_completed: ctx.steps.slice(0, 8),
      equipped_items: finalRes.equipped_items || [],
      summary: `已自动筹料并获得 ${effectiveTarget} x${effectiveAcquired}${stepSummary}`,
    };
  }

  const blockerReason = ctx.blockerReason || finalRes.reason || "acquire_failed";
  const blockerItem = ctx.blockerItem || finalRes.item || effectiveTarget;
  const partialNote =
    ctx.steps.length > 0 ? `；已完成: ${ctx.steps.slice(-4).join(" -> ")}` : "";
  return {
    ok: false,
    accepted: true,
    reason: blockerReason,
    item_name: effectiveTarget,
    requested_count: requestedCount,
    acquired_count: acquiredCount,
    owned_count: finalOwned,
    partial: ctx.steps.length > 0 || acquiredCount > 0,
    steps_completed: ctx.steps.slice(0, 8),
    blocker_item: blockerItem,
    summary: `自动筹料${effectiveTarget}在 ${blockerItem} 受阻(${blockerReason})${partialNote}`,
  };
}
