import vec3 from "vec3";
import { normalizeCanonicalItemId } from "./protocol.mjs";

export const EQUIPMENT_SLOTS = Object.freeze([
  "main_hand",
  "off_hand",
  "head",
  "chest",
  "legs",
  "feet",
]);

export const TOXIC_FOODS = new Set([
  "rotten_flesh",
  "spider_eye",
  "pufferfish",
  "poisonous_potato",
  "chorus_fruit",
  "suspicious_stew",
]);

export const SAFE_FOOD_SCORES = Object.freeze({
  golden_carrot: 20,
  cooked_beef: 19,
  cooked_porkchop: 19,
  cooked_mutton: 17,
  cooked_salmon: 17,
  rabbit_stew: 16,
  cooked_chicken: 15,
  cooked_rabbit: 15,
  cooked_cod: 14,
  baked_potato: 14,
  bread: 13,
  pumpkin_pie: 13,
  mushroom_stew: 13,
  beetroot_soup: 12,
  golden_apple: 11,
  enchanted_golden_apple: 10,
  honey_bottle: 10,
  apple: 9,
  carrot: 8,
  melon_slice: 6,
  sweet_berries: 6,
  glow_berries: 6,
  cookie: 5,
  dried_kelp: 5,
  beetroot: 4,
  potato: 4,
  beef: 4,
  porkchop: 4,
  mutton: 4,
  rabbit: 4,
  cod: 3,
  salmon: 3,
  tropical_fish: 2,
});

export const ARMOR_TIER_RANK = Object.freeze({
  leather: 1,
  golden: 2,
  chainmail: 3,
  iron: 4,
  diamond: 5,
  netherite: 6,
});

const ARMOR_SUFFIX_MAP = Object.freeze({
  helmet: { slotKey: "head", dest: "head" },
  chestplate: { slotKey: "chest", dest: "torso" },
  leggings: { slotKey: "legs", dest: "legs" },
  boots: { slotKey: "feet", dest: "feet" },
});

const ARMOR_RE = /^(leather|golden|chainmail|iron|diamond|netherite)_(helmet|chestplate|leggings|boots)$/;

const TOOL_TIER_RANK = Object.freeze({
  wooden: 1,
  golden: 2,
  stone: 3,
  iron: 4,
  diamond: 5,
  netherite: 6,
});

export const WEAPON_SCORES = Object.freeze({
  netherite_sword: 120,
  diamond_sword: 110,
  netherite_axe: 105,
  iron_sword: 95,
  diamond_axe: 90,
  stone_sword: 80,
  iron_axe: 75,
  golden_sword: 65,
  stone_axe: 60,
  wooden_sword: 50,
  golden_axe: 45,
  wooden_axe: 40,
});

export const SWORD_COOLDOWN_MS = 625;
export const AXE_COOLDOWN_MS = 1000;
export const DEFAULT_ATTACK_COOLDOWN_MS = 625;
export const DEFAULT_EAT_TIMEOUT_MS = 4000;
export const LOW_FOOD_THRESHOLD = 2;
export const UNREACHABLE_BLOCK_TTL_MS = 180_000;
export const UNREACHABLE_BLOCK_MAX_ENTRIES = 256;
export const AUTO_TORCH_COOLDOWN_MS = 8_000;
export const AUTO_TORCH_MIN_SPACING = 7;
export const AUTO_TORCH_LIGHT_THRESHOLD = 5;

const LIGHT_SOURCE_BLOCKS = new Set([
  "torch",
  "wall_torch",
  "soul_torch",
  "soul_wall_torch",
  "lantern",
  "soul_lantern",
  "campfire",
  "soul_campfire",
  "glowstone",
  "sea_lantern",
  "shroomlight",
  "jack_o_lantern",
]);

const TORCH_UNSAFE_GROUND_BLOCKS = new Set([
  "air",
  "cave_air",
  "void_air",
  "water",
  "flowing_water",
  "lava",
  "flowing_lava",
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
  "crafting_table",
  "furnace",
  "blast_furnace",
  "smoker",
  "chest",
  "trapped_chest",
  "barrel",
  "anvil",
  "enchanting_table",
  "glass",
  "ice",
  "packed_ice",
  "blue_ice",
]);

const HAZARD_FIRE_BLOCKS = new Set([
  "lava",
  "flowing_lava",
  "fire",
  "soul_fire",
  "magma_block",
  "campfire",
  "soul_campfire",
]);

const TORCH_AIR_BLOCKS = new Set(["air", "cave_air", "void_air"]);

export function createEmptyEquipment() {
  return {
    main_hand: "",
    off_hand: "",
    head: "",
    chest: "",
    legs: "",
    feet: "",
  };
}

/**
 * Extract ONLY the canonical English Minecraft item ID (e.g. "iron_sword").
 * Never reads custom displayName, anvil rename NBT, or components.
 */
export function extractCanonicalItemName(item) {
  if (!item || typeof item !== "object") return "";
  return normalizeCanonicalItemId(item.name);
}

export function getInventoryItems(bot) {
  if (!bot || !bot.inventory || typeof bot.inventory !== "object") return [];
  if (typeof bot.inventory.items === "function") {
    const raw = bot.inventory.items();
    return Array.isArray(raw) ? raw.filter(Boolean) : [];
  }
  if (Array.isArray(bot.inventory.slots)) {
    const start = Number.isInteger(bot.inventory.inventoryStart) ? bot.inventory.inventoryStart : 9;
    const end = Number.isInteger(bot.inventory.inventoryEnd) ? bot.inventory.inventoryEnd : 45;
    const out = [];
    for (let i = start; i < Math.min(end, bot.inventory.slots.length); i += 1) {
      const item = bot.inventory.slots[i];
      if (item) out.push(item);
    }
    return out;
  }
  return [];
}

function getDestSlotIndex(bot, destName, fallbackIndex) {
  if (typeof bot?.getEquipmentDestSlot === "function") {
    try {
      const idx = bot.getEquipmentDestSlot(destName);
      if (Number.isInteger(idx) && idx >= 0) return idx;
    } catch {
      // Fallback to standard player inventory slot index
    }
  }
  return fallbackIndex;
}

function getSlotItem(bot, destName, fallbackSlotIdx, entityEquipIdx) {
  const slots = bot?.inventory?.slots;
  if (Array.isArray(slots)) {
    const idx = getDestSlotIndex(bot, destName, fallbackSlotIdx);
    if (slots[idx]) return slots[idx];
  }
  const entEq = bot?.entity?.equipment;
  if (Array.isArray(entEq) && entEq[entityEquipIdx]) {
    return entEq[entityEquipIdx];
  }
  return null;
}

export function collectEquipmentSnapshot(bot) {
  const eq = createEmptyEquipment();
  if (!bot) return eq;

  let mainItem = bot.heldItem ?? null;
  if (!mainItem && Array.isArray(bot.inventory?.slots)) {
    const hotbarSlot = Number.isInteger(bot.quickBarSlot) ? bot.quickBarSlot : 0;
    const hotbarStart = Number.isInteger(bot.inventory.hotbarStart) ? bot.inventory.hotbarStart : 36;
    mainItem = bot.inventory.slots[hotbarStart + hotbarSlot] ?? null;
  }
  if (!mainItem && Array.isArray(bot.entity?.equipment)) {
    mainItem = bot.entity.equipment[0] ?? null;
  }

  eq.main_hand = extractCanonicalItemName(mainItem);
  eq.off_hand = extractCanonicalItemName(getSlotItem(bot, "off-hand", 45, 1));
  eq.head = extractCanonicalItemName(getSlotItem(bot, "head", 5, 5));
  eq.chest = extractCanonicalItemName(getSlotItem(bot, "torso", 6, 4));
  eq.legs = extractCanonicalItemName(getSlotItem(bot, "legs", 7, 3));
  eq.feet = extractCanonicalItemName(getSlotItem(bot, "feet", 8, 2));
  return eq;
}

export function collectInventorySnapshot(bot) {
  const equipment = collectEquipmentSnapshot(bot);
  if (!bot || !bot.inventory || typeof bot.inventory !== "object") {
    return {
      equipment,
      inventory_summary: [],
      free_slots: null,
    };
  }

  const counts = new Map();
  const items = getInventoryItems(bot);
  for (const item of items) {
    const name = extractCanonicalItemName(item);
    if (!name) continue;
    const rawCount = Number(item.count ?? 1);
    if (!Number.isFinite(rawCount) || rawCount <= 0) continue;
    const add = Math.floor(rawCount);
    if (add <= 0) continue;
    counts.set(name, Math.min(99999, (counts.get(name) ?? 0) + add));
  }

  const sorted = Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 16)
    .map(([name, count]) => `${name}:${count}`);

  let freeSlots = null;
  if (typeof bot.inventory.emptySlotCount === "function") {
    const rawFree = Number(bot.inventory.emptySlotCount());
    if (Number.isFinite(rawFree)) {
      freeSlots = Math.max(0, Math.min(36, Math.floor(rawFree)));
    }
  } else if (Array.isArray(bot.inventory.slots)) {
    const start = Number.isInteger(bot.inventory.inventoryStart) ? bot.inventory.inventoryStart : 9;
    const end = Number.isInteger(bot.inventory.inventoryEnd) ? bot.inventory.inventoryEnd : 45;
    let empty = 0;
    for (let i = start; i < end; i += 1) {
      if (!bot.inventory.slots[i]) empty += 1;
    }
    freeSlots = Math.max(0, Math.min(36, empty));
  } else if (typeof bot.inventory.items === "function") {
    freeSlots = Math.max(0, Math.min(36, 36 - items.length));
  }

  return {
    equipment,
    inventory_summary: sorted,
    free_slots: freeSlots,
  };
}

export function isSafeFoodItem(item, bot = null) {
  const name = extractCanonicalItemName(item);
  if (!name || TOXIC_FOODS.has(name)) return false;
  if (Object.hasOwn(SAFE_FOOD_SCORES, name)) return true;
  if (name !== "chicken" && bot?.registry?.foodsByName && Object.hasOwn(bot.registry.foodsByName, name)) {
    return true;
  }
  return false;
}

export function countSafeFood(bot) {
  if (!bot || !bot.inventory) return 0;
  let total = 0;
  for (const item of getInventoryItems(bot)) {
    if (!isSafeFoodItem(item, bot)) continue;
    const c = Number(item.count ?? 1);
    if (Number.isFinite(c) && c > 0) {
      total += Math.floor(c);
    }
  }
  return total;
}

export function pickBestSafeFood(bot, { health = null } = {}) {
  let best = null;
  let bestScore = -Infinity;
  for (const item of getInventoryItems(bot)) {
    if (!isSafeFoodItem(item, bot)) continue;
    const name = extractCanonicalItemName(item);
    let score = Object.hasOwn(SAFE_FOOD_SCORES, name)
      ? SAFE_FOOD_SCORES[name]
      : Number(bot?.registry?.foodsByName?.[name]?.effectiveQuality ?? 3);
    if (health !== null && health <= 8) {
      if (name === "golden_apple") score += 15;
      else if (name === "enchanted_golden_apple") score += 12;
    }
    if (score > bestScore) {
      bestScore = score;
      best = item;
    }
  }
  return best;
}

export function shouldTriggerAutoEat(health, food) {
  if (typeof food !== "number" || !Number.isFinite(food)) return false;
  if (food <= 14) return true;
  if (typeof health === "number" && Number.isFinite(health) && health <= 14 && food < 20) {
    return true;
  }
  return false;
}

export async function performAutoEat(
  bot,
  lockState,
  { health = null, eatTimeoutMs = DEFAULT_EAT_TIMEOUT_MS, onBeforeEat = null } = {},
) {
  if (!bot || !lockState) return { ate: false, reason: "no_bot" };
  if (lockState.eating) return { ate: false, reason: "already_eating" };

  const foodItem = pickBestSafeFood(bot, { health });
  if (!foodItem) {
    return { ate: false, reason: "no_safe_food", safeFoodCount: 0 };
  }
  if (typeof bot.equip !== "function" || typeof bot.consume !== "function") {
    return { ate: false, reason: "consume_unavailable", safeFoodCount: countSafeFood(bot) };
  }

  const foodName = extractCanonicalItemName(foodItem);
  const prevHeldName = extractCanonicalItemName(bot.heldItem);
  const prevQuickBarSlot = Number.isInteger(bot.quickBarSlot) ? bot.quickBarSlot : null;

  lockState.eating = true;
  let timer = null;
  let timedOut = false;
  let failed = false;

  try {
    if (typeof onBeforeEat === "function") {
      onBeforeEat();
    }
    await bot.equip(foodItem, "hand");
    const boundedTimeout = Math.max(20, Number.isFinite(eatTimeoutMs) ? eatTimeoutMs : DEFAULT_EAT_TIMEOUT_MS);
    await Promise.race([
      Promise.resolve(bot.consume()),
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          try {
            bot.deactivateItem?.();
          } catch {
            // Ignore deactivate errors
          }
          reject(new Error("eat_timeout"));
        }, boundedTimeout);
      }),
    ]);
  } catch {
    failed = true;
  } finally {
    if (timer) clearTimeout(timer);
    try {
      if (prevHeldName && prevHeldName !== foodName) {
        const restorable = getInventoryItems(bot).find(
          (it) => extractCanonicalItemName(it) === prevHeldName,
        );
        if (restorable) {
          await bot.equip(restorable, "hand");
        } else if (prevQuickBarSlot !== null && typeof bot.setQuickBarSlot === "function") {
          bot.setQuickBarSlot(prevQuickBarSlot);
        }
      } else if (prevQuickBarSlot !== null && typeof bot.setQuickBarSlot === "function") {
        bot.setQuickBarSlot(prevQuickBarSlot);
      }
    } catch {
      // Best-effort main-hand restoration
    }
    lockState.eating = false;
  }

  return {
    ate: !timedOut && !failed,
    timedOut,
    foodItem: foodName,
    safeFoodCount: countSafeFood(bot),
  };
}

export function getArmorInfo(itemName) {
  const clean = normalizeCanonicalItemId(itemName);
  if (!clean) return null;
  if (clean === "turtle_helmet") {
    return { slotKey: "head", dest: "head", rank: 3.5 };
  }
  const match = ARMOR_RE.exec(clean);
  if (!match) return null;
  const [, tier, piece] = match;
  const rank = ARMOR_TIER_RANK[tier] ?? 0;
  const slotMeta = ARMOR_SUFFIX_MAP[piece];
  if (!rank || !slotMeta) return null;
  return { slotKey: slotMeta.slotKey, dest: slotMeta.dest, rank };
}

export async function autoEquipBestGear(bot, { includeShield = true } = {}) {
  if (!bot || !bot.inventory || typeof bot.equip !== "function") return [];

  const equipped = [];
  const current = collectEquipmentSnapshot(bot);
  const items = getInventoryItems(bot);

  const bestBySlot = new Map();
  for (const item of items) {
    const name = extractCanonicalItemName(item);
    const info = getArmorInfo(name);
    if (!info) continue;
    const prev = bestBySlot.get(info.slotKey);
    if (!prev || info.rank > prev.rank) {
      bestBySlot.set(info.slotKey, { item, name, ...info });
    }
  }

  for (const slotKey of ["head", "chest", "legs", "feet"]) {
    const candidate = bestBySlot.get(slotKey);
    if (!candidate) continue;
    const curRank = getArmorInfo(current[slotKey])?.rank ?? 0;
    if (candidate.rank > curRank) {
      try {
        await bot.equip(candidate.item, candidate.dest);
        equipped.push(candidate.name);
      } catch {
        // Ignore individual armor equip failures
      }
    }
  }

  if (includeShield && current.off_hand !== "shield") {
    const shield = items.find((it) => extractCanonicalItemName(it) === "shield");
    if (shield) {
      try {
        await bot.equip(shield, "off-hand");
        equipped.push("shield");
      } catch {
        // Ignore shield equip failure
      }
    }
  }

  return equipped;
}

function inferPreferredToolSuffix(blockName) {
  const clean = normalizeCanonicalItemId(blockName);
  if (!clean) return null;
  if (
    clean.endsWith("_ore") ||
    clean === "ancient_debris" ||
    clean === "stone" ||
    clean === "cobblestone" ||
    clean === "deepslate" ||
    clean === "obsidian" ||
    clean === "netherrack" ||
    clean.includes("stone") ||
    clean.includes("deepslate") ||
    clean.includes("bricks") ||
    clean.includes("terracotta") ||
    clean.includes("concrete") ||
    clean === "furnace" ||
    clean === "anvil" ||
    clean === "basalt" ||
    clean === "andesite" ||
    clean === "diorite" ||
    clean === "granite"
  ) {
    return "pickaxe";
  }
  if (
    clean.endsWith("_log") ||
    clean.endsWith("_wood") ||
    clean.endsWith("_planks") ||
    clean.endsWith("_stem") ||
    clean === "crafting_table" ||
    clean === "chest" ||
    clean === "barrel" ||
    clean.includes("sign") ||
    clean.includes("fence") ||
    clean.includes("door") ||
    clean === "bookshelf"
  ) {
    return "axe";
  }
  if (
    clean === "dirt" ||
    clean === "grass_block" ||
    clean === "sand" ||
    clean === "red_sand" ||
    clean === "gravel" ||
    clean === "clay" ||
    clean === "soul_sand" ||
    clean === "soul_soil" ||
    clean === "snow" ||
    clean === "snow_block" ||
    clean === "mud" ||
    clean === "mycelium" ||
    clean === "podzol"
  ) {
    return "shovel";
  }
  if (
    clean.endsWith("_leaves") ||
    clean === "hay_block" ||
    clean === "moss_block" ||
    clean === "sponge" ||
    clean === "sculk"
  ) {
    return "hoe";
  }
  return null;
}

export async function equipBestHarvestTool(bot, block) {
  if (!bot || !block || typeof bot.equip !== "function") return null;
  const heldName = extractCanonicalItemName(bot.heldItem);

  if (typeof bot.pathfinder?.bestHarvestTool === "function") {
    try {
      const tool = bot.pathfinder.bestHarvestTool(block);
      if (tool) {
        const toolName = extractCanonicalItemName(tool);
        if (toolName && toolName !== heldName) {
          await bot.equip(tool, "hand");
        }
        return toolName || null;
      }
    } catch {
      // Fall through to heuristic tool selection
    }
  }

  const suffix = inferPreferredToolSuffix(block.name);
  if (!suffix) return null;

  let best = null;
  let bestRank = 0;
  for (const item of getInventoryItems(bot)) {
    const name = extractCanonicalItemName(item);
    if (!name.endsWith(`_${suffix}`)) continue;
    const tier = name.slice(0, -(suffix.length + 1));
    const rank = TOOL_TIER_RANK[tier] ?? 0;
    if (rank > bestRank) {
      bestRank = rank;
      best = { item, name };
    }
  }

  if (!best) return null;
  if (best.name !== heldName) {
    try {
      await bot.equip(best.item, "hand");
    } catch {
      return null;
    }
  }
  return best.name;
}

export function getWeaponScore(itemName) {
  const clean = normalizeCanonicalItemId(itemName);
  return Object.hasOwn(WEAPON_SCORES, clean) ? WEAPON_SCORES[clean] : 0;
}

export function getWeaponAttackCooldownMs(itemName) {
  const clean = normalizeCanonicalItemId(itemName);
  if (clean.endsWith("_sword")) return SWORD_COOLDOWN_MS;
  if (clean.endsWith("_axe")) return AXE_COOLDOWN_MS;
  return DEFAULT_ATTACK_COOLDOWN_MS;
}

export function createWaterFloatState() {
  return {
    floating: false,
    lastCheckMs: 0,
  };
}

/**
 * Keep the bot floating whenever submerged in water without interfering with
 * normal dry-land jumping or pathfinder control.
 */
export function updateWaterFloatControl(bot, floatState) {
  if (!bot || !floatState) return false;
  let inWater = Boolean(bot.entity?.isInWater);
  if (!inWater && bot.entity?.position && typeof bot.blockAt === "function") {
    try {
      const p = bot.entity.position;
      const feetBlk = bot.blockAt({
        x: Math.floor(p.x),
        y: Math.floor(p.y),
        z: Math.floor(p.z),
      });
      const feetName = normalizeCanonicalItemId(feetBlk?.name);
      if (feetName === "water" || feetName === "flowing_water" || feetName === "bubble_column") {
        inWater = true;
      }
    } catch {
      // Ignore blockAt errors on partial mocks
    }
  }

  if (inWater) {
    if (!floatState.floating) {
      try {
        bot.setControlState?.("jump", true);
        floatState.floating = true;
      } catch {
        // Ignore control errors
      }
    }
    return true;
  }

  if (floatState.floating) {
    try {
      bot.setControlState?.("jump", false);
    } catch {
      // Ignore control errors
    }
    floatState.floating = false;
  }
  return false;
}

export async function equipBestWeapon(bot) {
  if (!bot) return "";
  const heldName = extractCanonicalItemName(bot.heldItem);
  let bestScore = getWeaponScore(heldName);
  let bestItem = null;
  let bestName = heldName;

  for (const item of getInventoryItems(bot)) {
    const name = extractCanonicalItemName(item);
    const score = getWeaponScore(name);
    if (score > bestScore) {
      bestScore = score;
      bestItem = item;
      bestName = name;
    }
  }

  if (bestItem && typeof bot.equip === "function") {
    try {
      await bot.equip(bestItem, "hand");
    } catch {
      return heldName;
    }
  }
  return bestName;
}

/**
 * TTL-bounded memory of unreachable block coordinates so gather/mine/acquire
 * skip stubborn cliff/wall-blocked trees and ore veins across turns.
 */
export class UnreachableBlockBlacklist {
  constructor({
    ttlMs = UNREACHABLE_BLOCK_TTL_MS,
    maxEntries = UNREACHABLE_BLOCK_MAX_ENTRIES,
  } = {}) {
    this.ttlMs = Math.max(1_000, Number.isFinite(ttlMs) ? ttlMs : UNREACHABLE_BLOCK_TTL_MS);
    this.maxEntries = Math.max(16, Number.isFinite(maxEntries) ? Math.floor(maxEntries) : UNREACHABLE_BLOCK_MAX_ENTRIES);
    this._entries = new Map();
  }

  static keyOf(coords) {
    if (!coords) return "";
    if (Array.isArray(coords) && coords.length >= 3) {
      const x = Math.floor(Number(coords[0]));
      const y = Math.floor(Number(coords[1]));
      const z = Math.floor(Number(coords[2]));
      if (Number.isFinite(x) && Number.isFinite(y) && Number.isFinite(z)) {
        return `${x},${y},${z}`;
      }
      return "";
    }
    if (typeof coords === "object") {
      const x = Math.floor(Number(coords.x));
      const y = Math.floor(Number(coords.y));
      const z = Math.floor(Number(coords.z));
      if (Number.isFinite(x) && Number.isFinite(y) && Number.isFinite(z)) {
        return `${x},${y},${z}`;
      }
    }
    return "";
  }

  prune(nowMs = Date.now()) {
    for (const [key, expiresAt] of this._entries.entries()) {
      if (expiresAt <= nowMs) {
        this._entries.delete(key);
      }
    }
    while (this._entries.size > this.maxEntries) {
      const oldestKey = this._entries.keys().next().value;
      if (oldestKey === undefined) break;
      this._entries.delete(oldestKey);
    }
  }

  mark(coords, { nowMs = Date.now(), ttlMs = this.ttlMs } = {}) {
    const key = UnreachableBlockBlacklist.keyOf(coords);
    if (!key) return false;
    const dur = Math.max(1_000, Number.isFinite(ttlMs) ? ttlMs : this.ttlMs);
    this._entries.delete(key);
    this._entries.set(key, nowMs + dur);
    if (this._entries.size > this.maxEntries) {
      this.prune(nowMs);
    }
    return true;
  }

  markColumn(coords, { nowMs = Date.now(), ttlMs = this.ttlMs, yMin = -2, yMax = 3 } = {}) {
    const key = UnreachableBlockBlacklist.keyOf(coords);
    if (!key) return 0;
    const [cx, cy, cz] = key.split(",").map(Number);
    let added = 0;
    for (let dy = yMin; dy <= yMax; dy += 1) {
      if (this.mark([cx, cy + dy, cz], { nowMs, ttlMs })) {
        added += 1;
      }
    }
    return added;
  }

  has(coords, nowMs = Date.now()) {
    const key = UnreachableBlockBlacklist.keyOf(coords);
    if (!key) return false;
    const expiresAt = this._entries.get(key);
    if (expiresAt === undefined) return false;
    if (expiresAt <= nowMs) {
      this._entries.delete(key);
      return false;
    }
    return true;
  }

  size(nowMs = Date.now()) {
    this.prune(nowMs);
    return this._entries.size;
  }

  clear() {
    this._entries.clear();
  }
}

export function createAutoTorchState() {
  return {
    lastPlacedAtMs: -Infinity,
    lastPlacedPos: null,
  };
}

export function hasTorchInInventory(bot) {
  for (const item of getInventoryItems(bot)) {
    const name = extractCanonicalItemName(item);
    if (name === "torch" || name === "soul_torch") {
      return true;
    }
  }
  return false;
}

function findNearbyLightSource(bot, origin, radius = 5) {
  if (!bot || !origin) return false;
  if (typeof bot.findBlock === "function") {
    try {
      const existing = bot.findBlock({
        matching: (b) => Boolean(b && LIGHT_SOURCE_BLOCKS.has(normalizeCanonicalItemId(b.name))),
        maxDistance: radius,
      });
      if (existing) return true;
    } catch {
      // Fall through to blockAt scan
    }
  }
  if (typeof bot.blockAt === "function") {
    const bx = Math.floor(origin.x ?? 0);
    const by = Math.floor(origin.y ?? 0);
    const bz = Math.floor(origin.z ?? 0);
    for (let dx = -2; dx <= 2; dx += 1) {
      for (let dz = -2; dz <= 2; dz += 1) {
        for (let dy = -1; dy <= 2; dy += 1) {
          const blk = bot.blockAt(vec3(bx + dx, by + dy, bz + dz));
          if (blk && LIGHT_SOURCE_BLOCKS.has(normalizeCanonicalItemId(blk.name))) {
            return true;
          }
        }
      }
    }
  }
  return false;
}

function isSafeTorchGround(block) {
  if (!block) return false;
  const name = normalizeCanonicalItemId(block.name);
  if (!name || TORCH_UNSAFE_GROUND_BLOCKS.has(name)) return false;
  if (name.endsWith("_leaves") || name.endsWith("_bed") || name.endsWith("_door")) return false;
  if (block.boundingBox && block.boundingBox !== "block") return false;
  return true;
}

/**
 * Automatically place a torch when exploring caves or walking in dark areas.
 * Requires explicit numeric light readings from the block so bare mocks never
 * accidentally trigger placements, and restores the previously held main-hand item.
 */
export async function tryAutoPlaceTorch(
  bot,
  torchState,
  {
    nowMs = Date.now(),
    timeOfDay = "",
    sheltered = false,
    eating = false,
    cooldownMs = AUTO_TORCH_COOLDOWN_MS,
    minSpacing = AUTO_TORCH_MIN_SPACING,
  } = {},
) {
  if (!bot || !torchState || sheltered || eating) {
    return { placed: false, reason: "skipped" };
  }
  const origin = bot.entity?.position;
  if (
    !origin ||
    typeof bot.blockAt !== "function" ||
    typeof bot.placeBlock !== "function" ||
    typeof bot.equip !== "function"
  ) {
    return { placed: false, reason: "unavailable" };
  }
  if (nowMs - torchState.lastPlacedAtMs < cooldownMs) {
    return { placed: false, reason: "cooldown" };
  }
  const bx = Math.floor(origin.x ?? 0);
  const by = Math.floor(origin.y ?? 0);
  const bz = Math.floor(origin.z ?? 0);
  if (Array.isArray(torchState.lastPlacedPos) && torchState.lastPlacedPos.length === 3) {
    const distFromLast = Math.hypot(
      bx - torchState.lastPlacedPos[0],
      by - torchState.lastPlacedPos[1],
      bz - torchState.lastPlacedPos[2],
    );
    if (distFromLast < minSpacing) {
      return { placed: false, reason: "too_close_to_last" };
    }
  }

  const torchItem = getInventoryItems(bot).find((it) => {
    const name = extractCanonicalItemName(it);
    return name === "torch" || name === "soul_torch";
  });
  if (!torchItem) {
    return { placed: false, reason: "no_torch" };
  }

  const feetBlock = bot.blockAt(vec3(bx, by, bz));
  const blockLight =
    typeof feetBlock?.light === "number" && Number.isFinite(feetBlock.light)
      ? feetBlock.light
      : null;
  const skyLight =
    typeof feetBlock?.skyLight === "number" && Number.isFinite(feetBlock.skyLight)
      ? feetBlock.skyLight
      : null;

  if (blockLight === null) {
    return { placed: false, reason: "light_unknown" };
  }
  const isUndergroundDark =
    skyLight !== null && skyLight <= 4 && blockLight <= AUTO_TORCH_LIGHT_THRESHOLD;
  const isNightDark =
    (timeOfDay === "night" || timeOfDay === "sunset") && blockLight <= AUTO_TORCH_LIGHT_THRESHOLD - 1;
  const isGeneralDark = skyLight === null && blockLight <= AUTO_TORCH_LIGHT_THRESHOLD - 1;
  if (!isUndergroundDark && !isNightDark && !isGeneralDark) {
    return { placed: false, reason: "bright_enough" };
  }

  if (findNearbyLightSource(bot, origin, 5)) {
    return { placed: false, reason: "nearby_light_exists" };
  }

  const offsets = [
    [0, 0],
    [1, 0],
    [-1, 0],
    [0, 1],
    [0, -1],
  ];
  const prevHeldName = extractCanonicalItemName(bot.heldItem);
  const prevQuickBarSlot = Number.isInteger(bot.quickBarSlot) ? bot.quickBarSlot : null;
  const torchName = extractCanonicalItemName(torchItem);

  for (const [dx, dz] of offsets) {
    const tx = bx + dx;
    const ty = by;
    const tz = bz + dz;
    const targetBlk = bot.blockAt(vec3(tx, ty, tz));
    const groundBlk = bot.blockAt(vec3(tx, ty - 1, tz));
    if (!targetBlk || !TORCH_AIR_BLOCKS.has(normalizeCanonicalItemId(targetBlk.name))) continue;
    if (!isSafeTorchGround(groundBlk)) continue;

    try {
      await bot.equip(torchItem, "hand");
      if (typeof bot.lookAt === "function") {
        try {
          await bot.lookAt(vec3(tx + 0.5, ty, tz + 0.5), true);
        } catch {
          // Ignore lookAt errors
        }
      }
      await bot.placeBlock(groundBlk, vec3(0, 1, 0));
      torchState.lastPlacedAtMs = nowMs;
      torchState.lastPlacedPos = [tx, ty, tz];
      return { placed: true, pos: [tx, ty, tz], item: torchName };
    } catch {
      // Try next offset
    } finally {
      try {
        if (prevHeldName && prevHeldName !== torchName) {
          const restorable = getInventoryItems(bot).find(
            (it) => extractCanonicalItemName(it) === prevHeldName,
          );
          if (restorable) {
            await bot.equip(restorable, "hand");
          } else if (prevQuickBarSlot !== null && typeof bot.setQuickBarSlot === "function") {
            bot.setQuickBarSlot(prevQuickBarSlot);
          }
        } else if (prevQuickBarSlot !== null && typeof bot.setQuickBarSlot === "function") {
          bot.setQuickBarSlot(prevQuickBarSlot);
        }
      } catch {
        // Best-effort main-hand restoration
      }
    }
  }
  return { placed: false, reason: "no_valid_spot" };
}

export function createHazardState() {
  return {
    activeHazard: "",
    lastEscapeAtMs: -Infinity,
  };
}

/**
 * Inspect immediate life-threatening environmental hazards (lava/fire/magma or
 * drowning/low oxygen) for the Layer 0 self-preservation priority stack.
 */
export function detectEnvironmentalHazard(bot) {
  if (!bot) return { hazard: "", severity: 0 };
  const origin = bot.entity?.position;
  let feetName = "";
  let headName = "";
  let belowName = "";

  if (origin && typeof bot.blockAt === "function") {
    try {
      const bx = Math.floor(origin.x ?? 0);
      const by = Math.floor(origin.y ?? 0);
      const bz = Math.floor(origin.z ?? 0);
      feetName = normalizeCanonicalItemId(bot.blockAt(vec3(bx, by, bz))?.name);
      headName = normalizeCanonicalItemId(bot.blockAt(vec3(bx, by + 1, bz))?.name);
      belowName = normalizeCanonicalItemId(bot.blockAt(vec3(bx, by - 1, bz))?.name);
    } catch {
      // Ignore blockAt failures on partial mocks
    }
  }

  const inLava =
    feetName === "lava" ||
    feetName === "flowing_lava" ||
    headName === "lava" ||
    headName === "flowing_lava" ||
    Boolean(bot.entity?.isInLava);
  if (inLava) {
    return { hazard: "hazard_fire", detail: "in_lava", severity: 3 };
  }

  const onFireMeta =
    Array.isArray(bot.entity?.metadata) &&
    typeof bot.entity.metadata[0] === "number" &&
    (bot.entity.metadata[0] & 0x01) !== 0;
  const inFireBlock =
    HAZARD_FIRE_BLOCKS.has(feetName) ||
    HAZARD_FIRE_BLOCKS.has(headName) ||
    belowName === "magma_block";
  if (bot.entity?.isOnFire === true || onFireMeta || inFireBlock) {
    return { hazard: "hazard_fire", detail: inFireBlock ? "fire_block" : "on_fire", severity: 2 };
  }

  const headInWater = headName === "water" || headName === "flowing_water";
  const oxygen =
    typeof bot.oxygenLevel === "number" && Number.isFinite(bot.oxygenLevel)
      ? bot.oxygenLevel
      : null;
  if (
    (oxygen !== null && oxygen <= 6) ||
    (headInWater && oxygen !== null && oxygen <= 10)
  ) {
    return { hazard: "hazard_water", detail: "low_oxygen", severity: 2 };
  }

  return { hazard: "", severity: 0 };
}
