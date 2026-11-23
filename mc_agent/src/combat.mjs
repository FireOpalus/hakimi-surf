import pathfinderPkg from "mineflayer-pathfinder";
import { normalizeCanonicalItemId } from "./protocol.mjs";
import {
  collectEquipmentSnapshot,
  equipBestWeapon,
  extractCanonicalItemName,
  getWeaponAttackCooldownMs,
} from "./survival.mjs";

const { Movements, goals } = pathfinderPkg;
const { GoalFollow, GoalNearXZ } = goals;

export const HOSTILE_MOBS = new Set([
  "zombie",
  "skeleton",
  "creeper",
  "spider",
  "cave_spider",
  "enderman",
  "witch",
  "pillager",
  "drowned",
  "husk",
  "stray",
  "slime",
  "phantom",
  "bogged",
  "breeze",
  "blaze",
  "piglin_brute",
  "vindicator",
  "evoker",
  "ravager",
  "warden",
  "zombified_piglin",
]);

export const PROTECTED_ENTITIES = new Set([
  "player",
  "iron_golem",
  "snow_golem",
  "villager",
  "wandering_trader",
  "wolf",
  "cat",
  "parrot",
  "horse",
  "donkey",
  "mule",
  "llama",
  "trader_llama",
  "camel",
  "allay",
  "axolotl",
  "fox",
  "bee",
  "dolphin",
  "panda",
  "polar_bear",
  "ocelot",
  "sniffer",
  "armadillo",
  "cow",
  "pig",
  "sheep",
  "chicken",
  "rabbit",
  "turtle",
  "frog",
  "goat",
  "strider",
  "squid",
  "glow_squid",
  "bat",
  "cod",
  "salmon",
  "tropical_fish",
  "pufferfish",
  "mooshroom",
  "armor_stand",
  "item_frame",
  "glow_item_frame",
]);

export const AVOID_ALWAYS_MOBS = new Set(["warden", "ravager", "piglin_brute"]);
export const RANGED_SHIELD_MOBS = new Set(["skeleton", "stray", "bogged"]);
export const RANGED_HOSTILE_MOBS = new Set([
  "pillager",
  "skeleton",
  "stray",
  "bogged",
  "witch",
  "evoker",
  "blaze",
  "breeze",
]);

export const GUARD_PLAYER_RADIUS = 10;
export const SELF_DEFENSE_RADIUS = 7;
export const MAX_LEASH_FROM_PLAYER = 14;
export const CREEPER_SAFE_DISTANCE = 6;
export const AVOID_HIGH_DANGER_RADIUS = 12;
export const MELEE_EVADE_RADIUS = 8;
export const RANGED_EVADE_RADIUS = 18;
export const MELEE_FLEE_DISTANCE = 14;
export const RANGED_FLEE_DISTANCE = 26;
export const RETREAT_HEALTH_THRESHOLD = 7;
export const RESUME_HEALTH_THRESHOLD = 12;
export const MELEE_REACH = 3.2;

export function createCombatState() {
  return {
    retreating: false,
    shieldRaised: false,
    lastAttackAtMs: -Infinity,
    activeTargetId: null,
  };
}

export function distanceBetween(a, b) {
  if (!a || !b) return Infinity;
  if (typeof a.distanceTo === "function") {
    const d = a.distanceTo(b);
    return Number.isFinite(d) ? d : Infinity;
  }
  return Math.hypot((b.x ?? 0) - (a.x ?? 0), (b.y ?? 0) - (a.y ?? 0), (b.z ?? 0) - (a.z ?? 0));
}

export function isTamedOrOwnedEntity(entity) {
  if (!entity || typeof entity !== "object") return false;
  if (entity.tamed === true || entity.isTamed === true) return true;
  if (typeof entity.owner === "string" && entity.owner.length > 0) return true;
  if (typeof entity.ownerUUID === "string" && entity.ownerUUID.length > 0) return true;
  if (Array.isArray(entity.metadata)) {
    for (const meta of entity.metadata) {
      if (meta && typeof meta === "object" && (meta.tamed === true || meta.owner)) {
        return true;
      }
    }
  }
  return false;
}

export function isEndermanProvoked(entity) {
  if (!entity || typeof entity !== "object") return false;
  if (
    entity.isScreaming === true ||
    entity.angry === true ||
    entity.aggro === true ||
    entity.provoked === true
  ) {
    return true;
  }
  if (Array.isArray(entity.metadata)) {
    if (entity.metadata[17] === true || entity.metadata[18] === true) {
      return true;
    }
  }
  return false;
}

/**
 * Classify an entity under strict zero-PvP, neutral/pet protection,
 * creeper standoff, and high-danger avoidance rules.
 *
 * Returns one of: "ignore" | "protected" | "creeper" | "avoid" | "engageable"
 */
export function classifyCombatEntity(entity, bot = null) {
  if (!entity || typeof entity !== "object" || entity === bot?.entity || !entity.position) {
    return "ignore";
  }
  // Zero PvP iron law
  if (
    entity.type === "player" ||
    (typeof entity.username === "string" && entity.username.trim().length > 0)
  ) {
    return "protected";
  }
  // Tamed pet protection
  if (isTamedOrOwnedEntity(entity)) {
    return "protected";
  }

  const name = normalizeCanonicalItemId(String(entity.name || ""));
  if (!name || PROTECTED_ENTITIES.has(name)) {
    return "protected";
  }
  if (name === "creeper") {
    return "creeper";
  }
  if (AVOID_ALWAYS_MOBS.has(name)) {
    return "avoid";
  }
  if (name === "enderman") {
    return isEndermanProvoked(entity) ? "engageable" : "avoid";
  }
  if (HOSTILE_MOBS.has(name)) {
    return "engageable";
  }
  return "protected";
}

export function applyNoDigMovements(bot) {
  if (!bot) return null;
  if (!bot.pathfinder && typeof bot.loadPlugin === "function" && bot.registry?.blocksByName) {
    bot.loadPlugin(pathfinderPkg.pathfinder);
  }
  if (!bot.pathfinder || typeof bot.pathfinder.setMovements !== "function") return null;
  if (bot.registry?.blocksByName) {
    try {
      const movements = new Movements(bot);
      movements.canDig = false;
      movements.allow1by1towers = false;
      movements.canPlaceOn = false;
      if (Array.isArray(movements.scafoldingBlocks)) {
        movements.scafoldingBlocks = [];
      }
      bot.pathfinder.setMovements(movements);
      return movements;
    } catch {
      // Fall through to lightweight no-dig movements object on partial registries
    }
  }
  const safeMovements = {
    canDig: false,
    allow1by1towers: false,
    canPlaceOn: false,
    scafoldingBlocks: [],
  };
  bot.pathfinder.setMovements(safeMovements);
  return safeMovements;
}

export function hasShieldEquipped(bot) {
  return collectEquipmentSnapshot(bot).off_hand === "shield";
}

export function raiseShield(bot, combatState) {
  if (!bot || !combatState || combatState.shieldRaised) return false;
  if (!hasShieldEquipped(bot) || typeof bot.activateItem !== "function") return false;
  try {
    bot.activateItem(true);
    combatState.shieldRaised = true;
    return true;
  } catch {
    return false;
  }
}

export function lowerShield(bot, combatState) {
  if (!combatState || !combatState.shieldRaised) return false;
  combatState.shieldRaised = false;
  try {
    bot?.deactivateItem?.();
  } catch {
    // Ignore deactivate errors
  }
  return true;
}

export function computeRetreatTarget(origin, threatPos, stepDist = 10, safeAnchorPos = null) {
  const ox = origin.x ?? 0;
  const oz = origin.z ?? 0;
  const tx0 = threatPos.x ?? 0;
  const tz0 = threatPos.z ?? 0;
  const dx = ox - tx0;
  const dz = oz - tz0;
  const len = Math.hypot(dx, dz) || 1;
  let rx = Math.round(ox + (dx / len) * stepDist);
  let rz = Math.round(oz + (dz / len) * stepDist);

  if (safeAnchorPos) {
    const threatToBot = distanceBetween(origin, threatPos);
    const threatToAnchor = distanceBetween(safeAnchorPos, threatPos);
    const botToAnchor = distanceBetween(origin, safeAnchorPos);
    if (botToAnchor <= 24 && threatToAnchor > threatToBot + 2 && threatToAnchor > CREEPER_SAFE_DISTANCE + 2) {
      rx = Math.round(safeAnchorPos.x ?? rx);
      rz = Math.round(safeAnchorPos.z ?? rz);
    }
  }
  return { x: rx, z: rz };
}

export function findNearestCreeper(bot, maxDist = CREEPER_SAFE_DISTANCE) {
  const origin = bot?.entity?.position;
  if (!origin) return null;
  let best = null;
  for (const ent of Object.values(bot.entities || {})) {
    if (classifyCombatEntity(ent, bot) !== "creeper") continue;
    const dist = distanceBetween(origin, ent.position);
    if (dist <= maxDist && (!best || dist < best.dist)) {
      best = { entity: ent, name: "creeper", dist };
    }
  }
  return best;
}

export function findNearestAvoidMob(bot, maxDist = AVOID_HIGH_DANGER_RADIUS) {
  const origin = bot?.entity?.position;
  if (!origin) return null;
  let best = null;
  for (const ent of Object.values(bot.entities || {})) {
    const name = normalizeCanonicalItemId(String(ent?.name || ""));
    if (!AVOID_ALWAYS_MOBS.has(name)) continue;
    if (classifyCombatEntity(ent, bot) !== "avoid") continue;
    const dist = distanceBetween(origin, ent.position);
    if (dist <= maxDist && (!best || dist < best.dist)) {
      best = { entity: ent, name, dist };
    }
  }
  return best;
}

export function findNearestAnyHostile(bot, maxDist = 12) {
  const origin = bot?.entity?.position;
  if (!origin) return null;
  let best = null;
  for (const ent of Object.values(bot.entities || {})) {
    const cls = classifyCombatEntity(ent, bot);
    if (cls !== "engageable" && cls !== "creeper" && cls !== "avoid") continue;
    const name = normalizeCanonicalItemId(String(ent.name || ""));
    if (name === "enderman" && !isEndermanProvoked(ent)) continue;
    const dist = distanceBetween(origin, ent.position);
    if (dist <= maxDist && (!best || dist < best.dist)) {
      best = { entity: ent, name, cls, dist };
    }
  }
  return best;
}

export function findGuardTargetHostile(bot, guardedPlayer = null) {
  const origin = bot?.entity?.position;
  if (!origin) return null;
  const playerPos = guardedPlayer?.entity?.position ?? null;

  let best = null;
  for (const ent of Object.values(bot.entities || {})) {
    if (classifyCombatEntity(ent, bot) !== "engageable") continue;
    const distToBot = distanceBetween(origin, ent.position);
    const distToPlayer = playerPos ? distanceBetween(playerPos, ent.position) : Infinity;

    if (playerPos) {
      // Never chase a mob beyond MAX_LEASH_FROM_PLAYER (14 blocks) from guarded player
      if (distToPlayer > MAX_LEASH_FROM_PLAYER) continue;
      // Only engage if within <= 10 blocks of guarded player OR <= 7 blocks of bot (self-defense)
      if (distToPlayer > GUARD_PLAYER_RADIUS && distToBot > SELF_DEFENSE_RADIUS) continue;
    } else if (distToBot > SELF_DEFENSE_RADIUS) {
      continue;
    }

    const name = normalizeCanonicalItemId(String(ent.name || ""));
    const priorityScore = playerPos ? Math.min(distToPlayer, distToBot + 1.5) : distToBot;
    if (!best || priorityScore < best.priorityScore) {
      best = { entity: ent, name, dist: distToBot, distToPlayer, priorityScore };
    }
  }
  return best;
}

export async function runGuardCombatStep({
  bot,
  combatState,
  guardedPlayer = null,
  health = null,
  food = null,
  nowMs,
  autoEquip = true,
  eating = false,
  lookAtEntityFn = null,
  onNeedHealEat = null,
  onCreeperAlert = null,
  onRetreatAlert = null,
}) {
  const origin = bot?.entity?.position;
  if (!origin) return { handled: false, inCombat: false };

  const playerPos = guardedPlayer?.entity?.position ?? null;

  // 0. Low-health retreat & heal hysteresis (<= 7 retreat, >= 12 resume)
  if (typeof health === "number" && Number.isFinite(health)) {
    if (health <= RETREAT_HEALTH_THRESHOLD) {
      combatState.retreating = true;
    } else if (combatState.retreating && health >= RESUME_HEALTH_THRESHOLD) {
      combatState.retreating = false;
    }
  }

  if (combatState.retreating) {
    lowerShield(bot, combatState);
    combatState.activeTargetId = null;
    const nearestThreat = findNearestAnyHostile(bot, 12);
    if (nearestThreat) {
      const dest = computeRetreatTarget(origin, nearestThreat.entity.position, 10, playerPos);
      applyNoDigMovements(bot);
      try {
        bot.pathfinder?.setGoal?.(new GoalNearXZ(dest.x, dest.z, 2), true);
      } catch {
        // Ignore pathfinder errors
      }
    }
    if (typeof onRetreatAlert === "function") {
      onRetreatAlert({ health, food });
    }
    if (typeof onNeedHealEat === "function" && (food === null || food < 20)) {
      onNeedHealEat();
    }
    return { handled: true, inCombat: false, action: "retreat_heal", activity: "残血撤退回血" };
  }

  // 1. Creeper safe standoff (> 6 blocks, NEVER melee!)
  const creeper = findNearestCreeper(bot, CREEPER_SAFE_DISTANCE);
  if (creeper) {
    lowerShield(bot, combatState);
    combatState.activeTargetId = null;
    const dest = computeRetreatTarget(origin, creeper.entity.position, 10, playerPos);
    applyNoDigMovements(bot);
    try {
      bot.pathfinder?.setGoal?.(new GoalNearXZ(dest.x, dest.z, 2), true);
    } catch {
      // Ignore pathfinder errors
    }
    if (typeof onCreeperAlert === "function") {
      onCreeperAlert(creeper);
    }
    return {
      handled: true,
      inCombat: false,
      action: "creeper_standoff",
      creeper,
      activity: "远离苦力怕",
    };
  }

  // 2. Avoid high-danger mobs (warden, ravager, piglin_brute)
  const avoidMob = findNearestAvoidMob(bot, AVOID_HIGH_DANGER_RADIUS);
  if (avoidMob) {
    lowerShield(bot, combatState);
    combatState.activeTargetId = null;
    const dest = computeRetreatTarget(origin, avoidMob.entity.position, 14, playerPos);
    applyNoDigMovements(bot);
    try {
      bot.pathfinder?.setGoal?.(new GoalNearXZ(dest.x, dest.z, 2), true);
    } catch {
      // Ignore pathfinder errors
    }
    return {
      handled: true,
      inCombat: false,
      action: "avoid_danger",
      avoidMob,
      activity: "避开高危怪物",
    };
  }

  // 3. Enforce max leash distance (<= 14 blocks from guarded player)
  if (playerPos && distanceBetween(origin, playerPos) > MAX_LEASH_FROM_PLAYER) {
    lowerShield(bot, combatState);
    combatState.activeTargetId = null;
    applyNoDigMovements(bot);
    try {
      bot.pathfinder?.setGoal?.(new GoalFollow(guardedPlayer.entity, 3), true);
    } catch {
      // Ignore pathfinder errors
    }
    return {
      handled: true,
      inCombat: false,
      action: "leash_return",
      activity: `归队护卫:${guardedPlayer.username}`,
    };
  }

  // 4. Engage valid hostile within guard (<=10) / self-defense (<=7) & leash (<=14)
  const target = findGuardTargetHostile(bot, guardedPlayer);
  if (!target) {
    lowerShield(bot, combatState);
    combatState.activeTargetId = null;
    return { handled: false, inCombat: false };
  }

  // Re-verify safety classification immediately before any engagement
  if (classifyCombatEntity(target.entity, bot) !== "engageable") {
    lowerShield(bot, combatState);
    combatState.activeTargetId = null;
    return { handled: false, inCombat: false };
  }

  combatState.activeTargetId = target.entity.id ?? target.name;
  let weaponName = extractCanonicalItemName(bot.heldItem);
  if (autoEquip && !eating) {
    weaponName = await equipBestWeapon(bot);
  }
  const cooldownMs = getWeaponAttackCooldownMs(weaponName);
  const isRangedSkeleton = RANGED_SHIELD_MOBS.has(target.name);
  const shieldReady = hasShieldEquipped(bot);

  if (typeof lookAtEntityFn === "function") {
    lookAtEntityFn(bot, target.entity);
  }

  if (target.dist > MELEE_REACH) {
    if (isRangedSkeleton && shieldReady && !eating) {
      raiseShield(bot, combatState);
    } else {
      lowerShield(bot, combatState);
    }
    applyNoDigMovements(bot);
    try {
      bot.pathfinder?.setGoal?.(new GoalFollow(target.entity, 2), true);
    } catch {
      // Ignore pathfinder errors
    }
    return {
      handled: true,
      inCombat: true,
      action: "approach_hostile",
      attacked: false,
      target,
      cooldownMs,
      activity: `护卫迎击:${target.name}`,
    };
  }

  // Within melee reach: enforce 1.9+ attack speed cooldown (sword ~625ms, axe ~1000ms)
  const elapsed = nowMs - combatState.lastAttackAtMs;
  if (elapsed >= cooldownMs && !eating) {
    if (classifyCombatEntity(target.entity, bot) !== "engageable") {
      lowerShield(bot, combatState);
      return { handled: false, inCombat: false };
    }
    lowerShield(bot, combatState);
    if (typeof bot.attack === "function") {
      try {
        bot.attack(target.entity);
        combatState.lastAttackAtMs = nowMs;
      } catch {
        // Ignore attack errors
      }
    }
    return {
      handled: true,
      inCombat: true,
      action: "attack_hostile",
      attacked: true,
      target,
      cooldownMs,
      activity: `护卫迎击:${target.name}`,
    };
  }

  if (isRangedSkeleton && shieldReady && !eating) {
    raiseShield(bot, combatState);
  }
  return {
    handled: true,
    inCombat: true,
    action: "combat_cooldown",
    attacked: false,
    target,
    cooldownMs,
    activity: `护卫迎击:${target.name}`,
  };
}
