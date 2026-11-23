import vec3 from "vec3";
import pathfinderPkg from "mineflayer-pathfinder";
import { normalizeCanonicalItemId, normalizePlayerId } from "./protocol.mjs";
import {
  extractCanonicalItemName,
  equipBestHarvestTool,
  equipBestWeapon,
  getArmorInfo,
  getInventoryItems,
  getWeaponAttackCooldownMs,
  getWeaponScore,
  isSafeFoodItem,
} from "./survival.mjs";

const { Movements, goals } = pathfinderPkg;
const { GoalNear } = goals;

export const MIN_HUNT_COUNT = 1;
export const MAX_HUNT_COUNT = 3;
export const MIN_TRANSFER_COUNT = 1;
export const MAX_TRANSFER_COUNT = 64;
export const HUNT_SEARCH_RADIUS = 24;
export const CHEST_SEARCH_RADIUS = 16;
export const BED_SEARCH_RADIUS = 16;
export const GIVE_MAX_DISTANCE = 24;

export const HUNTABLE_PASSIVE_MOBS = new Set([
  "cow",
  "pig",
  "sheep",
  "chicken",
  "rabbit",
  "mooshroom",
]);

export const PROTECTED_MOBS = new Set([
  "player",
  "villager",
  "wandering_trader",
  "wolf",
  "cat",
  "ocelot",
  "horse",
  "donkey",
  "mule",
  "llama",
  "trader_llama",
  "camel",
  "fox",
  "axolotl",
  "iron_golem",
  "snow_golem",
  "allay",
  "parrot",
  "dolphin",
  "panda",
  "bee",
  "sniffer",
  "armadillo",
]);

export const SAFE_PLACEABLE_BLOCKS = new Set([
  "torch",
  "soul_torch",
  "lantern",
  "soul_lantern",
  "campfire",
  "crafting_table",
  "furnace",
  "smoker",
  "blast_furnace",
  "chest",
  "barrel",
  "cobblestone",
  "stone",
  "dirt",
  "oak_planks",
  "spruce_planks",
  "birch_planks",
  "jungle_planks",
  "acacia_planks",
  "dark_oak_planks",
  "mangrove_planks",
  "cherry_planks",
  "oak_door",
  "spruce_door",
  "birch_door",
  "ladder",
  "hay_block",
]);

export const PICKAXE_HARVEST_TIER = Object.freeze({
  wooden: 1,
  golden: 1,
  stone: 2,
  iron: 3,
  diamond: 4,
  netherite: 5,
});

const TIER_TO_PICKAXE_NAME = Object.freeze({
  1: "wooden_pickaxe",
  2: "stone_pickaxe",
  3: "iron_pickaxe",
  4: "diamond_pickaxe",
});

const BLOCK_MIN_PICKAXE_TIER = Object.freeze({
  stone: 1,
  cobblestone: 1,
  deepslate: 1,
  cobbled_deepslate: 1,
  andesite: 1,
  diorite: 1,
  granite: 1,
  sandstone: 1,
  red_sandstone: 1,
  netherrack: 1,
  basalt: 1,
  blackstone: 1,
  coal_ore: 1,
  deepslate_coal_ore: 1,
  nether_quartz_ore: 1,
  nether_gold_ore: 1,
  furnace: 1,
  blast_furnace: 1,
  smoker: 1,
  stone_bricks: 1,
  iron_ore: 2,
  deepslate_iron_ore: 2,
  copper_ore: 2,
  deepslate_copper_ore: 2,
  lapis_ore: 2,
  deepslate_lapis_ore: 2,
  raw_iron_block: 2,
  iron_block: 2,
  gold_ore: 3,
  deepslate_gold_ore: 3,
  diamond_ore: 3,
  deepslate_diamond_ore: 3,
  emerald_ore: 3,
  deepslate_emerald_ore: 3,
  redstone_ore: 3,
  deepslate_redstone_ore: 3,
  obsidian: 4,
  crying_obsidian: 4,
  ancient_debris: 4,
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

const EXPOSED_NEIGHBOR_BLOCKS = new Set([
  ...REPLACEABLE_BLOCKS,
  "water",
  "torch",
  "wall_torch",
  "soul_torch",
  "lantern",
  "rail",
  "vine",
  "oak_leaves",
  "birch_leaves",
  "spruce_leaves",
  "jungle_leaves",
  "acacia_leaves",
  "dark_oak_leaves",
  "cherry_leaves",
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

const CHEST_BLOCK_NAMES = new Set(["chest", "trapped_chest", "barrel"]);

const DEPOSIT_PROTECTED_ITEMS = new Set([
  "shield",
  "torch",
  "soul_torch",
  "crafting_table",
  "furnace",
  "water_bucket",
  "bucket",
  "compass",
  "clock",
]);

export function isBedName(name) {
  const clean = normalizeCanonicalItemId(name);
  return Boolean(clean && clean.endsWith("_bed"));
}

export function isSafePlaceableBlock(itemName) {
  const clean = normalizeCanonicalItemId(itemName);
  if (!clean) return false;
  return SAFE_PLACEABLE_BLOCKS.has(clean) || isBedName(clean);
}

/**
 * Configure pathfinder Movements safely so normal navigation never griefs
 * player bases (`canDig=false`, `allow1by1towers=false`, empty scaffolding).
 */
export function applySafeMovements(bot, { allowDig = false } = {}) {
  if (!bot) return null;
  if (!bot.pathfinder && typeof bot.loadPlugin === "function" && bot.registry?.blocksByName) {
    bot.loadPlugin(pathfinderPkg.pathfinder);
  }
  if (!bot.pathfinder || typeof bot.pathfinder.setMovements !== "function") return null;
  if (bot.registry?.blocksByName) {
    try {
      const movements = new Movements(bot);
      movements.canDig = Boolean(allowDig);
      movements.allow1by1towers = false;
      movements.canPlaceOn = Boolean(allowDig);
      if ("canOpenDoors" in movements) movements.canOpenDoors = true;
      if ("dontMineUnderFallingBlock" in movements) movements.dontMineUnderFallingBlock = true;
      if (!allowDig && Array.isArray(movements.scafoldingBlocks)) {
        movements.scafoldingBlocks = [];
      }
      bot.pathfinder.setMovements(movements);
      return movements;
    } catch {
      // Fall through to lightweight safe movements object on partial registries
    }
  }
  const safeMovements = {
    canDig: Boolean(allowDig),
    allow1by1towers: false,
    canPlaceOn: Boolean(allowDig),
    canOpenDoors: true,
    scafoldingBlocks: [],
  };
  bot.pathfinder.setMovements(safeMovements);
  return safeMovements;
}

/**
 * Verify that the bot holds or carries a high enough pickaxe tier to obtain
 * drops from `blockName`. Prevents 35s timeouts or 0-drop mining with wood/hand.
 */
export function checkHarvestToolTier(bot, blockName) {
  const clean = normalizeCanonicalItemId(blockName);
  if (!clean || !Object.hasOwn(BLOCK_MIN_PICKAXE_TIER, clean)) {
    return { ok: true, minTier: 0, haveTier: 0, bestTool: "" };
  }
  if (!bot?.inventory || typeof bot.inventory !== "object") {
    return { ok: true, skipped: true, minTier: 0, haveTier: 0, bestTool: "" };
  }
  const minTier = BLOCK_MIN_PICKAXE_TIER[clean];
  const requiredTool = TIER_TO_PICKAXE_NAME[minTier] || "wooden_pickaxe";

  let haveTier = 0;
  let bestTool = "";
  const candidates = [];
  if (bot?.heldItem) candidates.push(bot.heldItem);
  for (const it of getInventoryItems(bot)) {
    candidates.push(it);
  }

  for (const item of candidates) {
    const name = extractCanonicalItemName(item);
    if (!name.endsWith("_pickaxe")) continue;
    const prefix = name.slice(0, -"_pickaxe".length);
    const tier = PICKAXE_HARVEST_TIER[prefix] ?? 0;
    if (tier > haveTier) {
      haveTier = tier;
      bestTool = name;
    }
  }

  if (haveTier < minTier) {
    return {
      ok: false,
      reason: "tool_tier_too_low",
      requiredTool,
      bestTool,
      minTier,
      haveTier,
    };
  }
  return { ok: true, minTier, haveTier, bestTool };
}

/**
 * Return true when at least one of the 6 cardinal neighbors of `pos` is air or
 * non-solid so `mine`/`gather` prefers reachable surface/cave-wall blocks over
 * deeply buried stone/ore that causes pathfinder timeouts.
 */
export function hasExposedFace(bot, pos) {
  if (!bot || typeof bot.blockAt !== "function" || !pos) return true;
  const x = Math.floor(Array.isArray(pos) ? pos[0] : pos.x);
  const y = Math.floor(Array.isArray(pos) ? pos[1] : pos.y);
  const z = Math.floor(Array.isArray(pos) ? pos[2] : pos.z);
  if (![x, y, z].every(Number.isFinite)) return true;

  const dirs = [
    [1, 0, 0],
    [-1, 0, 0],
    [0, 1, 0],
    [0, -1, 0],
    [0, 0, 1],
    [0, 0, -1],
  ];
  let checkedAny = false;
  for (const [dx, dy, dz] of dirs) {
    let blk = null;
    try {
      blk = bot.blockAt(vec3(x + dx, y + dy, z + dz));
    } catch {
      blk = null;
    }
    if (!blk) continue;
    checkedAny = true;
    const name = normalizeCanonicalItemId(blk.name);
    if (EXPOSED_NEIGHBOR_BLOCKS.has(name) || blk.boundingBox === "empty") {
      return true;
    }
  }
  return !checkedAny;
}

export function parseHuntParams(params) {
  let targetMob = "";
  const rawMob = params?.target_mob ?? params?.mob ?? "";
  if (rawMob !== undefined && rawMob !== null && rawMob !== "") {
    if (typeof rawMob !== "string") return { error: "invalid_target_mob" };
    const clean = normalizeCanonicalItemId(rawMob);
    if (!clean || !HUNTABLE_PASSIVE_MOBS.has(clean)) {
      return { error: "mob_not_huntable" };
    }
    targetMob = clean;
  }

  const rawCount = params?.count === undefined ? 1 : params.count;
  if (
    typeof rawCount !== "number" ||
    !Number.isInteger(rawCount) ||
    rawCount < MIN_HUNT_COUNT ||
    rawCount > MAX_HUNT_COUNT
  ) {
    return { error: "invalid_hunt_count" };
  }
  return { targetMob, count: rawCount };
}

export function parseChestParams(params) {
  const rawMode = params?.mode === undefined || params?.mode === "" ? "deposit" : params.mode;
  if (typeof rawMode !== "string") return { error: "invalid_chest_mode" };
  const mode = rawMode.trim().toLowerCase();
  if (mode !== "deposit" && mode !== "withdraw" && mode !== "inspect") {
    return { error: "invalid_chest_mode" };
  }

  let itemName = "";
  if (params?.item_name !== undefined && params?.item_name !== null && params?.item_name !== "") {
    if (typeof params.item_name !== "string") return { error: "invalid_item_name" };
    const clean = normalizeCanonicalItemId(params.item_name);
    if (!clean) return { error: "invalid_item_name" };
    itemName = clean;
  }
  if (mode === "withdraw" && !itemName) {
    return { error: "item_name_required" };
  }

  const rawCount = params?.count === undefined ? 16 : params.count;
  if (
    typeof rawCount !== "number" ||
    !Number.isInteger(rawCount) ||
    rawCount < MIN_TRANSFER_COUNT ||
    rawCount > MAX_TRANSFER_COUNT
  ) {
    return { error: "invalid_chest_count" };
  }
  return { mode, itemName, count: rawCount };
}

export function parsePlaceParams(params) {
  const rawItem = params?.item_name ?? params?.block_type ?? "";
  if (typeof rawItem !== "string") return { error: "invalid_item_name" };
  const itemName = normalizeCanonicalItemId(rawItem);
  if (!itemName) return { error: "invalid_item_name" };
  if (!isSafePlaceableBlock(itemName)) return { error: "block_not_placeable" };
  return { itemName };
}

export function parseGiveParams(params) {
  const targetPlayer = normalizePlayerId(params?.target_player ?? params?.player ?? "");
  if (!targetPlayer) return { error: "invalid_target_player" };

  const rawItem = params?.item_name ?? "";
  if (typeof rawItem !== "string") return { error: "invalid_item_name" };
  const itemName = normalizeCanonicalItemId(rawItem);
  if (!itemName) return { error: "invalid_item_name" };

  const rawCount = params?.count === undefined ? 1 : params.count;
  if (
    typeof rawCount !== "number" ||
    !Number.isInteger(rawCount) ||
    rawCount < MIN_TRANSFER_COUNT ||
    rawCount > MAX_TRANSFER_COUNT
  ) {
    return { error: "invalid_give_count" };
  }
  return { targetPlayer, itemName, count: rawCount };
}

function sleepAbortable(ms, signal) {
  if (signal?.aborted) return Promise.resolve(false);
  const delay = Math.max(1, Number(ms) || 1);
  return new Promise((resolve) => {
    let done = false;
    const finish = (completed) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      if (signal && onAbort) {
        try {
          signal.removeEventListener("abort", onAbort);
        } catch {
          // Ignore listener removal errors
        }
      }
      resolve(completed);
    };
    const onAbort = () => finish(false);
    const timer = setTimeout(() => finish(true), delay);
    if (signal) {
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

function hasCustomEntityName(entity) {
  if (!entity || typeof entity !== "object") return false;
  if (typeof entity.customName === "string" && entity.customName.trim().length > 0) return true;
  if (entity.customName && typeof entity.customName === "object") {
    const text = String(entity.customName.text ?? entity.customName.extra?.[0]?.text ?? "").trim();
    if (text.length > 0) return true;
  }
  if (Array.isArray(entity.metadata) && entity.metadata[2]) {
    const raw = entity.metadata[2];
    if (typeof raw === "string" && raw.trim().length > 0 && raw !== '""') return true;
  }
  return false;
}

function isEntityAlive(bot, entity) {
  if (!entity || entity.isValid === false) return false;
  if (typeof entity.health === "number" && entity.health <= 0) return false;
  if (bot?.entities && entity.id !== undefined && !bot.entities[entity.id]) return false;
  return true;
}

function findNearestHuntableMob(bot, targetMob, maxDistance = HUNT_SEARCH_RADIUS) {
  const origin = bot?.entity?.position;
  if (!origin || !bot?.entities) return null;
  let best = null;
  let bestDist = Infinity;

  for (const ent of Object.values(bot.entities)) {
    if (!ent || ent === bot.entity || !ent.position) continue;
    if (ent.type === "player" || ent.username) continue;
    const mobName = normalizeCanonicalItemId(ent.name);
    if (!mobName || PROTECTED_MOBS.has(mobName) || !HUNTABLE_PASSIVE_MOBS.has(mobName)) {
      continue;
    }
    if (targetMob && mobName !== targetMob) continue;
    if (hasCustomEntityName(ent)) continue;

    const dist = Math.hypot(
      (ent.position.x ?? 0) - origin.x,
      (ent.position.y ?? 0) - origin.y,
      (ent.position.z ?? 0) - origin.z,
    );
    if (Number.isFinite(dist) && dist <= maxDistance && dist < bestDist) {
      bestDist = dist;
      best = { entity: ent, name: mobName, dist };
    }
  }
  return best;
}

function hasCreeperNearby(bot, safeDist = 6) {
  const origin = bot?.entity?.position;
  if (!origin || !bot?.entities) return false;
  for (const ent of Object.values(bot.entities)) {
    if (!ent || ent === bot.entity || !ent.position) continue;
    if (normalizeCanonicalItemId(ent.name) !== "creeper") continue;
    const dist = Math.hypot(
      (ent.position.x ?? 0) - origin.x,
      (ent.position.y ?? 0) - origin.y,
      (ent.position.z ?? 0) - origin.z,
    );
    if (Number.isFinite(dist) && dist <= safeDist) return true;
  }
  return false;
}

function findSafePlacementSpot(bot, { requireDouble = false } = {}) {
  if (!bot || !bot.entity?.position || typeof bot.blockAt !== "function") return null;
  const bx = Math.floor(bot.entity.position.x);
  const by = Math.floor(bot.entity.position.y);
  const bz = Math.floor(bot.entity.position.z);

  const isSpotFreeAndSupported = (tx, ty, tz) => {
    if (tx === bx && tz === bz && (ty === by || ty === by + 1)) return false;
    const targetBlk = bot.blockAt(vec3(tx, ty, tz));
    const groundBlk = bot.blockAt(vec3(tx, ty - 1, tz));
    if (!targetBlk || !groundBlk) return false;
    if (!REPLACEABLE_BLOCKS.has(normalizeCanonicalItemId(targetBlk.name))) return false;
    if (UNSAFE_GROUND_BLOCKS.has(normalizeCanonicalItemId(groundBlk.name))) return false;
    return { targetBlk, groundBlk, pos: [tx, ty, tz] };
  };

  for (const [dx, dz] of PLACE_OFFSETS_XZ) {
    for (const dy of [0, 1, -1]) {
      const tx = bx + dx;
      const ty = by + dy;
      const tz = bz + dz;
      const primary = isSpotFreeAndSupported(tx, ty, tz);
      if (!primary) continue;
      if (!requireDouble) return primary;
      // Beds require a second adjacent horizontal block (head of bed).
      for (const [hx, hz] of [
        [1, 0],
        [-1, 0],
        [0, 1],
        [0, -1],
      ]) {
        if (isSpotFreeAndSupported(tx + hx, ty, tz + hz)) {
          return primary;
        }
      }
    }
  }
  return null;
}

async function placeInventoryItemSafely(bot, item, { requireDouble = false, signal = null } = {}) {
  if (!bot || !item || typeof bot.equip !== "function" || typeof bot.placeBlock !== "function") {
    return { ok: false, reason: "place_unavailable" };
  }
  if (signal?.aborted) return { ok: false, reason: "mc_goal_cancelled" };
  const spot = findSafePlacementSpot(bot, { requireDouble });
  if (!spot) return { ok: false, reason: "no_safe_placement_spot" };

  try {
    await bot.equip(item, "hand");
    await bot.placeBlock(spot.groundBlk, vec3(0, 1, 0));
  } catch {
    return { ok: false, reason: "place_failed" };
  }
  const placedBlock =
    typeof bot.blockAt === "function" ? bot.blockAt(vec3(...spot.pos)) : { position: vec3(...spot.pos) };
  return { ok: true, pos: spot.pos, block: placedBlock };
}

export async function executeHuntAction(
  bot,
  params,
  {
    signal = null,
    gotoGoal = null,
    collectDrops = null,
    attackCooldownOverrideMs = null,
    maxSwingsPerMob = 14,
  } = {},
) {
  const parsed = parseHuntParams(params);
  if (parsed.error) {
    return { ok: false, reason: parsed.error, summary: `狩猎参数无效:${parsed.error}` };
  }
  const { targetMob, count } = parsed;
  if (!bot || !bot.entity?.position) {
    return { ok: false, reason: "bot_not_ready", summary: "角色尚未就绪" };
  }

  let huntedCount = 0;
  let lastMobName = targetMob || "";
  const weaponName = await equipBestWeapon(bot);
  const cooldownMs =
    typeof attackCooldownOverrideMs === "number" && attackCooldownOverrideMs > 0
      ? attackCooldownOverrideMs
      : getWeaponAttackCooldownMs(weaponName);

  for (let i = 0; i < count; i += 1) {
    if (signal?.aborted) break;
    if (typeof bot.health === "number" && bot.health <= 6) {
      return {
        ok: huntedCount > 0,
        hunted_count: huntedCount,
        target_mob: lastMobName,
        reason: huntedCount > 0 ? undefined : "low_health_abort",
        summary: huntedCount > 0 ? `已狩猎${lastMobName}x${huntedCount}（低血撤退）` : "生命偏低暂停狩猎",
      };
    }
    if (hasCreeperNearby(bot, 6)) {
      return {
        ok: huntedCount > 0,
        hunted_count: huntedCount,
        target_mob: lastMobName,
        reason: huntedCount > 0 ? undefined : "creeper_abort",
        summary: huntedCount > 0 ? `已狩猎${lastMobName}x${huntedCount}（避开苦力怕）` : "附近有苦力怕暂停狩猎",
      };
    }

    const found = findNearestHuntableMob(bot, targetMob, HUNT_SEARCH_RADIUS);
    if (!found) {
      if (huntedCount > 0) break;
      return {
        ok: false,
        hunted_count: 0,
        target_mob: targetMob,
        reason: "no_huntable_mob",
        summary: targetMob ? `附近未找到可狩猎的${targetMob}` : "附近未找到可狩猎的食用动物",
      };
    }

    const mob = found.entity;
    lastMobName = found.name;
    let lastPos = [mob.position.x, mob.position.y, mob.position.z];
    let killed = false;

    for (let swing = 0; swing < maxSwingsPerMob; swing += 1) {
      if (signal?.aborted) break;
      if (!isEntityAlive(bot, mob)) {
        killed = true;
        break;
      }
      if (typeof bot.health === "number" && bot.health <= 6) break;
      if (hasCreeperNearby(bot, 6)) break;

      const curOrigin = bot.entity?.position;
      const mPos = mob.position;
      if (!curOrigin || !mPos) break;
      lastPos = [mPos.x, mPos.y, mPos.z];

      const dist = Math.hypot(mPos.x - curOrigin.x, mPos.y - curOrigin.y, mPos.z - curOrigin.z);
      if (dist > 3.1 && typeof gotoGoal === "function") {
        try {
          await gotoGoal(new GoalNear(mPos.x, mPos.y, mPos.z, 2), signal);
        } catch {
          // Best-effort chase step; retry attack check if close enough
        }
      }
      if (signal?.aborted) break;
      if (!isEntityAlive(bot, mob)) {
        killed = true;
        break;
      }

      const afterOrigin = bot.entity?.position;
      const afterDist = afterOrigin && mob.position
        ? Math.hypot(
            mob.position.x - afterOrigin.x,
            mob.position.y - afterOrigin.y,
            mob.position.z - afterOrigin.z,
          )
        : dist;
      if (afterDist <= 3.5 && typeof bot.attack === "function") {
        if (typeof bot.lookAt === "function" && mob.position) {
          try {
            await bot.lookAt(vec3(mob.position.x, mob.position.y + 0.6, mob.position.z), true);
          } catch {
            // Ignore lookAt errors
          }
        }
        try {
          bot.attack(mob);
        } catch {
          break;
        }
        if (!isEntityAlive(bot, mob)) {
          killed = true;
          break;
        }
        const okWait = await sleepAbortable(cooldownMs, signal);
        if (!okWait) break;
      } else {
        const okWait = await sleepAbortable(Math.min(250, cooldownMs), signal);
        if (!okWait) break;
      }
    }

    if (killed || !isEntityAlive(bot, mob)) {
      huntedCount += 1;
      if (typeof collectDrops === "function") {
        await collectDrops(bot, lastPos, signal);
      }
    } else {
      break;
    }
  }

  if (huntedCount > 0) {
    return {
      ok: true,
      hunted_count: huntedCount,
      target_mob: lastMobName,
      summary: `已狩猎${lastMobName}x${huntedCount}并拾取掉落物`,
    };
  }
  return {
    ok: false,
    hunted_count: 0,
    target_mob: lastMobName || targetMob,
    reason: signal?.aborted ? "mc_goal_cancelled" : "hunt_failed",
    summary: "狩猎未能完成",
  };
}

export async function executeSleepAction(
  bot,
  _params,
  { signal = null, gotoGoal = null } = {},
) {
  if (!bot || !bot.entity?.position) {
    return { ok: false, reason: "bot_not_ready", summary: "角色尚未就绪" };
  }
  if (typeof bot.sleep !== "function") {
    return { ok: false, reason: "sleep_unavailable", summary: "当前客户端不支持睡觉动作" };
  }

  let bedBlock = null;
  let placedBed = false;

  if (typeof bot.findBlocks === "function") {
    const found =
      bot.findBlocks({
        matching: (b) => Boolean(b && isBedName(b.name)),
        point: bot.entity.position,
        maxDistance: BED_SEARCH_RADIUS,
        count: 4,
      }) || [];
    if (found.length > 0 && typeof bot.blockAt === "function") {
      bedBlock = bot.blockAt(found[0]);
    }
  }

  if (!bedBlock) {
    const bedItem = getInventoryItems(bot).find((it) => isBedName(extractCanonicalItemName(it)));
    if (bedItem) {
      const placed = await placeInventoryItemSafely(bot, bedItem, { requireDouble: true, signal });
      if (placed.ok) {
        placedBed = true;
        bedBlock = placed.block;
      }
    }
  }

  if (!bedBlock) {
    return {
      ok: false,
      reason: "no_bed_nearby",
      summary: "附近和背包都没有床",
    };
  }

  const bpos = bedBlock.position
    ? [Math.floor(bedBlock.position.x), Math.floor(bedBlock.position.y), Math.floor(bedBlock.position.z)]
    : [Math.floor(bot.entity.position.x), Math.floor(bot.entity.position.y), Math.floor(bot.entity.position.z)];

  if (typeof gotoGoal === "function") {
    try {
      await gotoGoal(new GoalNear(bpos[0], bpos[1], bpos[2], 2), signal);
    } catch {
      return {
        ok: false,
        reason: signal?.aborted ? "mc_goal_cancelled" : "bed_unreachable",
        bed_pos: bpos,
        summary: "无法走到床边",
      };
    }
  }
  if (signal?.aborted) {
    return { ok: false, reason: "mc_goal_cancelled", summary: "睡觉已取消" };
  }

  try {
    await bot.sleep(bedBlock);
  } catch (err) {
    const msg = String(err?.message || "").toLowerCase();
    let reason = "sleep_failed";
    if (msg.includes("night") || msg.includes("thunder") || msg.includes("day")) {
      reason = "not_night_or_thunder";
    } else if (msg.includes("monster")) {
      reason = "monsters_nearby";
    } else if (msg.includes("occupied")) {
      reason = "bed_occupied";
    } else if (msg.includes("far")) {
      reason = "bed_too_far";
    }
    return {
      ok: false,
      reason,
      placed_bed: placedBed,
      bed_pos: bpos,
      summary: `无法入睡:${reason}`,
    };
  }

  return {
    ok: true,
    slept: true,
    placed_bed: placedBed,
    bed_pos: bpos,
    summary: placedBed ? `已放床并入睡(${bpos.join(",")})` : `已在(${bpos.join(",")})入睡`,
  };
}

function summarizeContainerItems(container) {
  const raw =
    typeof container?.containerItems === "function"
      ? container.containerItems()
      : Array.isArray(container?.slots)
        ? container.slots.slice(0, container.inventoryStart ?? 27).filter(Boolean)
        : [];
  const counts = new Map();
  for (const item of raw) {
    const name = extractCanonicalItemName(item);
    if (!name) continue;
    const qty = Math.floor(Number(item.count ?? 1));
    if (qty > 0) {
      counts.set(name, (counts.get(name) ?? 0) + qty);
    }
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 12)
    .map(([name, qty]) => `${name}:${qty}`);
}

function selectAutoDepositCandidates(bot) {
  const items = getInventoryItems(bot);
  const bestToolBySuffix = new Map();
  let bestWeaponName = "";
  let bestWeaponScore = 0;
  const bestArmorBySlot = new Map();

  for (const item of items) {
    const name = extractCanonicalItemName(item);
    if (!name) continue;
    const wScore = getWeaponScore(name);
    if (wScore > bestWeaponScore) {
      bestWeaponScore = wScore;
      bestWeaponName = name;
    }
    const armor = getArmorInfo(name);
    if (armor) {
      const prev = bestArmorBySlot.get(armor.slotKey);
      if (!prev || armor.rank > prev.rank) {
        bestArmorBySlot.set(armor.slotKey, { name, rank: armor.rank });
      }
    }
    for (const suffix of ["_pickaxe", "_axe", "_shovel"]) {
      if (name.endsWith(suffix)) {
        const tier = PICKAXE_HARVEST_TIER[name.slice(0, -suffix.length)] ?? 0;
        const prev = bestToolBySuffix.get(suffix);
        if (!prev || tier > prev.tier) {
          bestToolBySuffix.set(suffix, { name, tier });
        }
      }
    }
  }

  const protectedNames = new Set(DEPOSIT_PROTECTED_ITEMS);
  if (bestWeaponName) protectedNames.add(bestWeaponName);
  for (const entry of bestArmorBySlot.values()) protectedNames.add(entry.name);
  for (const entry of bestToolBySuffix.values()) protectedNames.add(entry.name);

  let safeFoodKept = 0;
  const candidates = [];
  for (const item of items) {
    const name = extractCanonicalItemName(item);
    if (!name || isBedName(name) || protectedNames.has(name)) continue;
    const count = Math.floor(Number(item.count ?? 1));
    if (count <= 0) continue;
    if (isSafeFoodItem(item, bot)) {
      if (safeFoodKept < 16) {
        safeFoodKept += count;
        continue;
      }
    }
    candidates.push({ item, name, count });
  }
  return candidates.sort((a, b) => b.count - a.count).slice(0, 4);
}

export async function executeChestAction(
  bot,
  params,
  { signal = null, gotoGoal = null } = {},
) {
  const parsed = parseChestParams(params);
  if (parsed.error) {
    return { ok: false, reason: parsed.error, summary: `箱子参数无效:${parsed.error}` };
  }
  const { mode, itemName, count } = parsed;
  if (!bot || !bot.entity?.position) {
    return { ok: false, reason: "bot_not_ready", summary: "角色尚未就绪" };
  }

  const openFn =
    typeof bot.openContainer === "function"
      ? (blk) => bot.openContainer(blk)
      : typeof bot.openChest === "function"
        ? (blk) => bot.openChest(blk)
        : null;
  if (!openFn) {
    return { ok: false, reason: "chest_unavailable", summary: "当前客户端不支持箱子交互" };
  }

  let chestBlock = null;
  let placedChest = false;

  if (typeof bot.findBlocks === "function") {
    const found =
      bot.findBlocks({
        matching: (b) => Boolean(b && CHEST_BLOCK_NAMES.has(normalizeCanonicalItemId(b.name))),
        point: bot.entity.position,
        maxDistance: CHEST_SEARCH_RADIUS,
        count: 4,
      }) || [];
    if (found.length > 0 && typeof bot.blockAt === "function") {
      chestBlock = bot.blockAt(found[0]);
    }
  }

  if (!chestBlock && mode === "deposit") {
    const chestItem = getInventoryItems(bot).find((it) =>
      CHEST_BLOCK_NAMES.has(extractCanonicalItemName(it)),
    );
    if (chestItem) {
      const placed = await placeInventoryItemSafely(bot, chestItem, { signal });
      if (placed.ok) {
        placedChest = true;
        chestBlock = placed.block;
      }
    }
  }

  if (!chestBlock) {
    return {
      ok: false,
      mode,
      reason: "no_chest_nearby",
      summary: "附近未找到箱子或木桶",
    };
  }

  const cpos = chestBlock.position
    ? [
        Math.floor(chestBlock.position.x),
        Math.floor(chestBlock.position.y),
        Math.floor(chestBlock.position.z),
      ]
    : [
        Math.floor(bot.entity.position.x),
        Math.floor(bot.entity.position.y),
        Math.floor(bot.entity.position.z),
      ];

  if (typeof gotoGoal === "function") {
    try {
      await gotoGoal(new GoalNear(cpos[0], cpos[1], cpos[2], 3), signal);
    } catch {
      return {
        ok: false,
        mode,
        chest_pos: cpos,
        reason: signal?.aborted ? "mc_goal_cancelled" : "chest_unreachable",
        summary: "无法走到箱子旁",
      };
    }
  }
  if (signal?.aborted) {
    return { ok: false, mode, reason: "mc_goal_cancelled", summary: "箱子交互已取消" };
  }

  let container = null;
  try {
    container = await openFn(chestBlock);
    if (!container) {
      return { ok: false, mode, chest_pos: cpos, reason: "chest_open_failed", summary: "打开箱子失败" };
    }

    if (mode === "inspect") {
      const chestSummary = summarizeContainerItems(container);
      return {
        ok: true,
        mode,
        chest_pos: cpos,
        chest_summary: chestSummary,
        summary:
          chestSummary.length > 0
            ? `箱子内容:${chestSummary.slice(0, 6).join(",")}`
            : "箱子目前是空的",
      };
    }

    if (mode === "deposit") {
      if (typeof container.deposit !== "function") {
        return { ok: false, mode, chest_pos: cpos, reason: "chest_deposit_unavailable", summary: "无法存入箱子" };
      }
      let moved = 0;
      let movedName = itemName;
      if (itemName) {
        let remaining = count;
        for (const it of getInventoryItems(bot)) {
          if (remaining <= 0 || signal?.aborted) break;
          if (extractCanonicalItemName(it) !== itemName) continue;
          const qty = Math.min(remaining, Math.floor(Number(it.count ?? 1)));
          if (qty <= 0) continue;
          await container.deposit(it.type, it.metadata ?? null, qty);
          moved += qty;
          remaining -= qty;
        }
      } else {
        const candidates = selectAutoDepositCandidates(bot);
        const depositedNames = [];
        for (const entry of candidates) {
          if (signal?.aborted) break;
          const qty = Math.min(count, entry.count);
          if (qty <= 0) continue;
          try {
            await container.deposit(entry.item.type, entry.item.metadata ?? null, qty);
            moved += qty;
            depositedNames.push(entry.name);
          } catch {
            // Chest may be full; stop depositing further stacks
            break;
          }
        }
        movedName = depositedNames[0] || "";
      }

      if (moved <= 0) {
        return {
          ok: false,
          mode,
          item_name: itemName,
          chest_pos: cpos,
          reason: itemName ? "item_not_in_inventory" : "nothing_to_deposit",
          summary: itemName ? `背包里没有${itemName}可存入` : "背包里没有多余物资可存入",
        };
      }
      return {
        ok: true,
        mode,
        item_name: movedName,
        moved_count: moved,
        placed_chest: placedChest,
        chest_pos: cpos,
        chest_summary: summarizeContainerItems(container),
        summary: `已存入箱子${movedName ? `:${movedName}` : ""}x${moved}`,
      };
    }

    // mode === "withdraw"
    if (typeof container.withdraw !== "function") {
      return { ok: false, mode, chest_pos: cpos, reason: "chest_withdraw_unavailable", summary: "无法从箱子取出" };
    }
    const rawItems =
      typeof container.containerItems === "function"
        ? container.containerItems()
        : Array.isArray(container.slots)
          ? container.slots.slice(0, container.inventoryStart ?? 27).filter(Boolean)
          : [];
    let moved = 0;
    let remaining = count;
    for (const it of rawItems) {
      if (remaining <= 0 || signal?.aborted) break;
      if (extractCanonicalItemName(it) !== itemName) continue;
      const qty = Math.min(remaining, Math.floor(Number(it.count ?? 1)));
      if (qty <= 0) continue;
      await container.withdraw(it.type, it.metadata ?? null, qty);
      moved += qty;
      remaining -= qty;
    }

    if (moved <= 0) {
      return {
        ok: false,
        mode,
        item_name: itemName,
        chest_pos: cpos,
        chest_summary: summarizeContainerItems(container),
        reason: "item_not_in_chest",
        summary: `箱子里没有${itemName}`,
      };
    }
    return {
      ok: true,
      mode,
      item_name: itemName,
      moved_count: moved,
      chest_pos: cpos,
      chest_summary: summarizeContainerItems(container),
      summary: `已从箱子取出${itemName}x${moved}`,
    };
  } catch {
    return {
      ok: false,
      mode,
      item_name: itemName,
      chest_pos: cpos,
      reason: "chest_interaction_failed",
      summary: "箱子交互失败",
    };
  } finally {
    if (container && typeof container.close === "function") {
      try {
        container.close();
      } catch {
        // Ignore close errors
      }
    }
  }
}

export async function executePlaceAction(bot, params, { signal = null } = {}) {
  const parsed = parsePlaceParams(params);
  if (parsed.error) {
    return { ok: false, reason: parsed.error, summary: `放置参数无效:${parsed.error}` };
  }
  const { itemName } = parsed;
  if (!bot || !bot.entity?.position) {
    return { ok: false, reason: "bot_not_ready", summary: "角色尚未就绪" };
  }

  const item = getInventoryItems(bot).find((it) => extractCanonicalItemName(it) === itemName);
  if (!item) {
    return {
      ok: false,
      item_name: itemName,
      reason: "missing_item",
      summary: `背包里没有${itemName}`,
    };
  }

  const placed = await placeInventoryItemSafely(bot, item, {
    requireDouble: isBedName(itemName),
    signal,
  });
  if (!placed.ok) {
    return {
      ok: false,
      item_name: itemName,
      reason: placed.reason,
      summary: `放置${itemName}失败:${placed.reason}`,
    };
  }
  return {
    ok: true,
    item_name: itemName,
    placed_pos: placed.pos,
    summary: `已放置${itemName}(${placed.pos.join(",")})`,
  };
}

export function isDirectlyUnderfoot(bot, pos) {
  const origin = bot?.entity?.position;
  if (!origin || !pos) return false;
  const bx = Math.floor(origin.x);
  const by = Math.floor(origin.y);
  const bz = Math.floor(origin.z);
  const tx = Math.floor(Array.isArray(pos) ? pos[0] : pos.x);
  const ty = Math.floor(Array.isArray(pos) ? pos[1] : pos.y);
  const tz = Math.floor(Array.isArray(pos) ? pos[2] : pos.z);
  if (![bx, by, bz, tx, ty, tz].every(Number.isFinite)) return false;
  return tx === bx && tz === bz && (ty === by - 1 || ty === by - 2);
}

const SHELTER_CAP_BLOCKS = new Set([
  "dirt",
  "cobblestone",
  "cobbled_deepslate",
  "stone",
  "oak_planks",
  "spruce_planks",
  "birch_planks",
  "jungle_planks",
  "acacia_planks",
  "dark_oak_planks",
  "mangrove_planks",
  "cherry_planks",
  "sandstone",
  "netherrack",
  "andesite",
  "diorite",
  "granite",
]);

const SHELTER_DIGGABLE_TERRAIN = new Set([
  "dirt",
  "grass_block",
  "coarse_dirt",
  "podzol",
  "rooted_dirt",
  "mud",
  "stone",
  "cobblestone",
  "andesite",
  "diorite",
  "granite",
  "deepslate",
  "cobbled_deepslate",
  "sandstone",
  "red_sandstone",
  "clay",
  "snow_block",
  "netherrack",
]);

const SHELTER_HAZARD_BLOCKS = new Set([
  "water",
  "flowing_water",
  "lava",
  "flowing_lava",
  "fire",
  "soul_fire",
  "magma_block",
  "cactus",
  "powder_snow",
  "bubble_column",
]);

export function createShelterState() {
  return {
    active: false,
    capPos: null,
    holePos: null,
    surfaceY: null,
  };
}

export function parseShelterParams(params) {
  const rawMode = params?.mode === undefined || params?.mode === "" ? "auto" : params.mode;
  if (typeof rawMode !== "string") return { error: "invalid_shelter_mode" };
  const mode = rawMode.trim().toLowerCase();
  if (mode !== "enter" && mode !== "exit" && mode !== "auto") {
    return { error: "invalid_shelter_mode" };
  }
  return { mode };
}

function isHazardousBlock(blk) {
  if (!blk) return false;
  return SHELTER_HAZARD_BLOCKS.has(normalizeCanonicalItemId(blk.name));
}

function isSolidNonHazardWall(blk) {
  if (!blk) return false;
  const name = normalizeCanonicalItemId(blk.name);
  if (!name || UNSAFE_GROUND_BLOCKS.has(name) || SHELTER_HAZARD_BLOCKS.has(name)) return false;
  return blk.boundingBox !== "empty";
}

async function waitForShaftLanding(
  bot,
  targetFootY,
  { signal = null, timeoutMs = 1400, pollMs = 40 } = {},
) {
  // Only poll gravity settlement when the runtime exposes real physics onGround state.
  if (typeof bot.entity?.onGround !== "boolean") return true;
  const deadline = Date.now() + Math.max(0, timeoutMs);
  while (Date.now() < deadline) {
    if (signal?.aborted) return false;
    const curY = Number(bot.entity?.position?.y);
    if (Number.isFinite(curY) && curY <= targetFootY + 0.15 && bot.entity.onGround === true) {
      if (pollMs > 0) {
        await sleepAbortable(Math.min(120, pollMs * 3), signal);
      }
      return true;
    }
    const okWait = await sleepAbortable(Math.max(10, pollMs), signal);
    if (!okWait) return false;
  }
  const finalY = Number(bot.entity?.position?.y);
  return Number.isFinite(finalY) && finalY <= targetFootY + 0.25;
}

export async function executeShelterAction(
  bot,
  params,
  {
    signal = null,
    shelterState = null,
    timeOfDay = "",
    gotoGoal = null,
    autoEquip = true,
    settleTimeoutMs = 1400,
    settlePollMs = 40,
  } = {},
) {
  const parsed = parseShelterParams(params);
  if (parsed.error) {
    return { ok: false, reason: parsed.error, summary: `掩体参数无效:${parsed.error}` };
  }
  if (!bot || !bot.entity?.position || typeof bot.blockAt !== "function") {
    return { ok: false, reason: "bot_not_ready", summary: "角色尚未就绪" };
  }

  const state = shelterState || createShelterState();
  let effectiveMode = parsed.mode;
  if (effectiveMode === "auto") {
    effectiveMode =
      state.active && (timeOfDay === "day" || timeOfDay === "sunrise") ? "exit" : "enter";
  }

  const origin = bot.entity.position;
  const bx = Math.floor(origin.x);
  const by = Math.floor(origin.y);
  const bz = Math.floor(origin.z);

  const digBlockSafe = async (x, y, z) => {
    if (signal?.aborted || typeof bot.dig !== "function") return;
    const blk = bot.blockAt(vec3(x, y, z));
    if (!blk) return;
    const name = normalizeCanonicalItemId(blk.name);
    if (!name || REPLACEABLE_BLOCKS.has(name) || isHazardousBlock(blk)) return;
    if (!SHELTER_DIGGABLE_TERRAIN.has(name) && !SHELTER_CAP_BLOCKS.has(name)) return;
    if (!checkHarvestToolTier(bot, name).ok) return;
    if (autoEquip) {
      await equipBestHarvestTool(bot, blk);
    }
    try {
      await bot.dig(blk);
    } catch {
      // Best-effort exit dig
    }
  };

  const carveStaircaseOut = async (shaftX, footY, shaftZ, surfaceY) => {
    const dirs = [
      [1, 0],
      [-1, 0],
      [0, 1],
      [0, -1],
    ];
    let exitTarget = [shaftX, surfaceY, shaftZ];
    for (const [dx, dz] of dirs) {
      const s1a = bot.blockAt(vec3(shaftX + dx, footY + 1, shaftZ + dz));
      const s1b = bot.blockAt(vec3(shaftX + dx, footY + 2, shaftZ + dz));
      const s2a = bot.blockAt(vec3(shaftX + dx * 2, footY + 2, shaftZ + dz * 2));
      if (isHazardousBlock(s1a) || isHazardousBlock(s1b) || isHazardousBlock(s2a)) continue;
      await digBlockSafe(shaftX + dx, footY + 1, shaftZ + dz);
      await digBlockSafe(shaftX + dx, footY + 2, shaftZ + dz);
      await digBlockSafe(shaftX + dx, footY + 3, shaftZ + dz);
      await digBlockSafe(shaftX + dx * 2, footY + 2, shaftZ + dz * 2);
      await digBlockSafe(shaftX + dx * 2, footY + 3, shaftZ + dz * 2);
      exitTarget = [shaftX + dx * 3, Math.max(footY + 3, surfaceY), shaftZ + dz * 3];
      break;
    }
    if (typeof gotoGoal === "function" && !signal?.aborted) {
      try {
        await gotoGoal(new GoalNear(exitTarget[0], exitTarget[1], exitTarget[2], 2), signal);
      } catch {
        // Best-effort exit walk
      }
    }
  };

  if (effectiveMode === "exit") {
    const cap = Array.isArray(state.capPos) ? state.capPos : [bx, by + 2, bz];
    // 1. Open roof cap above head
    await digBlockSafe(cap[0], cap[1], cap[2]);
    await digBlockSafe(cap[0], cap[1] + 1, cap[2]);
    // 2. Carve a safe 2-step staircase out in the first hazard-free cardinal direction
    await carveStaircaseOut(bx, by, bz, cap[1] + 1);

    state.active = false;
    state.capPos = null;
    state.holePos = null;
    state.surfaceY = null;
    return {
      ok: true,
      mode: "exit",
      sheltered: false,
      summary: "已挖开封顶并离开夜间应急掩体",
    };
  }

  // effectiveMode === "enter"
  if (state.active) {
    return {
      ok: true,
      mode: "enter",
      sheltered: true,
      shelter_pos: state.holePos || [bx, by, bz],
      summary: "当前已在应急掩体内安全躲避",
    };
  }

  const feetBlk = bot.blockAt(vec3(bx, by, bz));
  if (bot.entity?.isInWater || isHazardousBlock(feetBlk)) {
    return {
      ok: false,
      mode: "enter",
      sheltered: false,
      reason: "unsafe_shelter_ground",
      summary: "当前位于水域或危险方块上，无法就地建掩体",
    };
  }

  const capItem = getInventoryItems(bot).find((it) =>
    SHELTER_CAP_BLOCKS.has(extractCanonicalItemName(it)),
  );
  if (!capItem) {
    return {
      ok: false,
      mode: "enter",
      sheltered: false,
      reason: "no_shelter_blocks",
      summary: "背包缺少泥土/圆石/木板等非重力封顶方块",
    };
  }

  if (
    typeof bot.dig !== "function" ||
    typeof bot.equip !== "function" ||
    typeof bot.placeBlock !== "function"
  ) {
    return {
      ok: false,
      mode: "enter",
      sheltered: false,
      reason: "shelter_unavailable",
      summary: "当前客户端不支持挖建掩体",
    };
  }

  // Verify 3-deep vertical shaft is natural terrain with solid safe floor at by-4
  // and no water/lava adjacent to any dug level.
  const b1 = bot.blockAt(vec3(bx, by - 1, bz));
  const b2 = bot.blockAt(vec3(bx, by - 2, bz));
  const b3 = bot.blockAt(vec3(bx, by - 3, bz));
  const floor4 = bot.blockAt(vec3(bx, by - 4, bz));
  const digTargets = [b1, b2, b3];

  for (const blk of digTargets) {
    const name = normalizeCanonicalItemId(blk?.name);
    if (!name || !SHELTER_DIGGABLE_TERRAIN.has(name)) {
      return {
        ok: false,
        mode: "enter",
        sheltered: false,
        reason: "unsafe_shelter_ground",
        summary: "脚下非自然泥土/石质地面，跳过挖坑以防破坏建筑",
      };
    }
    const tierCheck = checkHarvestToolTier(bot, name);
    if (!tierCheck.ok) {
      return {
        ok: false,
        mode: "enter",
        sheltered: false,
        reason: "tool_tier_too_low",
        required_tool: tierCheck.requiredTool,
        summary: `脚下是${name}，需要至少${tierCheck.requiredTool}才能挖掩体`,
      };
    }
  }

  if (!isSolidNonHazardWall(floor4)) {
    return {
      ok: false,
      mode: "enter",
      sheltered: false,
      reason: "unsafe_shelter_ground",
      summary: "脚下可能中空或临近危险方块，放弃挖掩体",
    };
  }

  const cardinalOffsets = [
    [1, 0],
    [-1, 0],
    [0, 1],
    [0, -1],
  ];
  for (const dy of [-1, -2, -3]) {
    for (const [dx, dz] of cardinalOffsets) {
      const sideBlk = bot.blockAt(vec3(bx + dx, by + dy, bz + dz));
      if (isHazardousBlock(sideBlk)) {
        return {
          ok: false,
          mode: "enter",
          sheltered: false,
          reason: "unsafe_shelter_ground",
          summary: "周边存在水域或岩浆，放弃挖掩体",
        };
      }
    }
  }

  // Collect all solid side wall candidates at cap height (by - 1) to attach roof cap
  const anchorCandidates = [];
  for (const [dx, dz] of cardinalOffsets) {
    const wall = bot.blockAt(vec3(bx + dx, by - 1, bz + dz));
    if (isSolidNonHazardWall(wall)) {
      anchorCandidates.push({ dx, dz, face: vec3(-dx, 0, -dz) });
    }
  }
  if (anchorCandidates.length === 0) {
    return {
      ok: false,
      mode: "enter",
      sheltered: false,
      reason: "no_shelter_wall_anchor",
      summary: "周围缺少可封顶的实体侧壁",
    };
  }

  // Dig 3 blocks straight down safely (zero fall damage: 3 blocks)
  let dugCount = 0;
  for (const blk of digTargets) {
    if (signal?.aborted) {
      return { ok: false, mode: "enter", sheltered: false, reason: "mc_goal_cancelled", summary: "掩体已取消" };
    }
    if (autoEquip) {
      await equipBestHarvestTool(bot, blk);
    }
    try {
      await bot.dig(blk);
      dugCount += 1;
    } catch {
      if (dugCount >= 2) {
        const curFootY = Math.min(by - dugCount, Math.floor(Number(bot.entity?.position?.y ?? by - dugCount)));
        await carveStaircaseOut(bx, curFootY, bz, by);
      }
      return {
        ok: false,
        mode: "enter",
        sheltered: false,
        reason: "shelter_dig_failed",
        summary: "挖掘应急掩体失败",
      };
    }
  }

  if (signal?.aborted) {
    return { ok: false, mode: "enter", sheltered: false, reason: "mc_goal_cancelled", summary: "掩体已取消" };
  }

  // Wait for gravity settlement to the bottom of the 3-block shaft (by - 3) before placing cap
  // at (by - 1), otherwise the player's own head hitbox still overlaps (by - 1) and rejects placeBlock.
  await waitForShaftLanding(bot, by - 3, {
    signal,
    timeoutMs: settleTimeoutMs,
    pollMs: settlePollMs,
  });

  if (signal?.aborted) {
    return { ok: false, mode: "enter", sheltered: false, reason: "mc_goal_cancelled", summary: "掩体已取消" };
  }

  // Seal roof cap at (bx, by - 1, bz) — try all valid solid side wall anchors
  const freshCapItem =
    getInventoryItems(bot).find((it) => SHELTER_CAP_BLOCKS.has(extractCanonicalItemName(it))) ||
    capItem;
  let sealed = false;
  try {
    await bot.equip(freshCapItem, "hand");
    for (const { dx, dz, face } of anchorCandidates) {
      const liveWall = bot.blockAt(vec3(bx + dx, by - 1, bz + dz));
      if (!isSolidNonHazardWall(liveWall)) continue;
      if (typeof bot.lookAt === "function" && liveWall.position) {
        try {
          await bot.lookAt(
            vec3(
              liveWall.position.x + 0.5 + face.x * 0.5,
              liveWall.position.y + 0.5,
              liveWall.position.z + 0.5 + face.z * 0.5,
            ),
            true,
          );
        } catch {
          // Ignore lookAt errors
        }
      }
      try {
        await bot.placeBlock(liveWall, face);
        sealed = true;
        break;
      } catch {
        // Try next solid side wall anchor
      }
    }
  } catch {
    sealed = false;
  }

  if (!sealed) {
    // Never leave the bot trapped in an unsealed 3-deep open shaft: carve staircase out to surface.
    const curFootY = Math.min(by - 3, Math.floor(Number(bot.entity?.position?.y ?? by - 3)));
    await carveStaircaseOut(bx, curFootY, bz, by);
    return {
      ok: false,
      mode: "enter",
      sheltered: false,
      escaped: true,
      reason: "shelter_seal_failed",
      summary: "掩体封顶失败，已自动挖出逃生台阶返回地面",
    };
  }

  state.active = true;
  state.capPos = [bx, by - 1, bz];
  state.holePos = [bx, by - 3, bz];
  state.surfaceY = by;
  return {
    ok: true,
    mode: "enter",
    sheltered: true,
    shelter_pos: state.holePos,
    cap_pos: state.capPos,
    summary: `已就地挖好1x1x2应急掩体并封顶(${state.holePos.join(",")})，安全苟到天亮`,
  };
}

export async function executeGiveAction(
  bot,
  params,
  { signal = null, gotoGoal = null, allowedPlayers = null } = {},
) {
  const parsed = parseGiveParams(params);
  if (parsed.error) {
    return { ok: false, reason: parsed.error, summary: `递物参数无效:${parsed.error}` };
  }
  const { targetPlayer, itemName, count } = parsed;
  if (!bot || !bot.entity?.position) {
    return { ok: false, reason: "bot_not_ready", summary: "角色尚未就绪" };
  }
  if (
    allowedPlayers &&
    allowedPlayers.size > 0 &&
    !allowedPlayers.has(targetPlayer.toLowerCase())
  ) {
    return {
      ok: false,
      target_player: targetPlayer,
      item_name: itemName,
      reason: "player_not_allowed",
      summary: `目标玩家${targetPlayer}不在允许列表`,
    };
  }

  const playerEntry =
    bot.players?.[targetPlayer] ||
    Object.values(bot.players || {}).find(
      (p) =>
        p &&
        typeof p.username === "string" &&
        p.username.toLowerCase() === targetPlayer.toLowerCase(),
    );
  const resolvedPlayerName = normalizePlayerId(playerEntry?.username || targetPlayer) || targetPlayer;
  const targetEntity = playerEntry?.entity;
  if (!targetEntity?.position) {
    return {
      ok: false,
      target_player: targetPlayer,
      item_name: itemName,
      reason: "player_not_found",
      summary: `附近未找到玩家${targetPlayer}`,
    };
  }

  const origin = bot.entity.position;
  const initDist = Math.hypot(
    targetEntity.position.x - origin.x,
    targetEntity.position.y - origin.y,
    targetEntity.position.z - origin.z,
  );
  if (!Number.isFinite(initDist) || initDist > GIVE_MAX_DISTANCE) {
    return {
      ok: false,
      target_player: targetPlayer,
      item_name: itemName,
      reason: "player_too_far",
      summary: `玩家${targetPlayer}距离过远`,
    };
  }

  const matching = getInventoryItems(bot).filter((it) => extractCanonicalItemName(it) === itemName);
  const totalHave = matching.reduce((acc, it) => acc + Math.max(0, Math.floor(Number(it.count ?? 1))), 0);
  if (totalHave <= 0) {
    return {
      ok: false,
      target_player: targetPlayer,
      item_name: itemName,
      reason: "item_not_in_inventory",
      summary: `背包里没有${itemName}`,
    };
  }

  if (typeof bot.toss !== "function") {
    return { ok: false, reason: "toss_unavailable", summary: "当前客户端不支持丢出物品" };
  }

  if (initDist > 3.0 && typeof gotoGoal === "function") {
    try {
      await gotoGoal(
        new GoalNear(targetEntity.position.x, targetEntity.position.y, targetEntity.position.z, 2),
        signal,
      );
    } catch {
      return {
        ok: false,
        target_player: targetPlayer,
        item_name: itemName,
        reason: signal?.aborted ? "mc_goal_cancelled" : "player_unreachable",
        summary: `无法走到${targetPlayer}身边`,
      };
    }
  }
  if (signal?.aborted) {
    return { ok: false, reason: "mc_goal_cancelled", summary: "递物已取消" };
  }

  if (typeof bot.lookAt === "function" && targetEntity.position) {
    try {
      await bot.lookAt(
        vec3(targetEntity.position.x, targetEntity.position.y + 1.4, targetEntity.position.z),
        true,
      );
    } catch {
      // Ignore lookAt errors
    }
  }

  const actualCount = Math.min(totalHave, count);
  try {
    const first = matching[0];
    await bot.toss(first.type, first.metadata ?? null, actualCount);
  } catch {
    return {
      ok: false,
      target_player: targetPlayer,
      item_name: itemName,
      reason: "toss_failed",
      summary: `递出${itemName}失败`,
    };
  }

  return {
    ok: true,
    target_player: resolvedPlayerName,
    item_name: itemName,
    given_count: actualCount,
    summary: `已递给${resolvedPlayerName} ${itemName}x${actualCount}`,
  };
}
