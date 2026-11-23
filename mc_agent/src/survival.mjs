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
