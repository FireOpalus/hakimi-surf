import crypto from "node:crypto";

export const MAX_WS_PAYLOAD_BYTES = 262144;
export const BRIDGE_VERSION = "2.0.0";

const FORBIDDEN_TARGET_KEYS = new Set([
  "host",
  "port",
  "url",
  "server",
  "mc_host",
  "mc_port",
  "server_host",
  "server_port",
]);

const MAX_JSON_DEPTH = 64;
const ANSI_CSI_RE = /\x1b\[[0-9;]*[A-Za-z]/g;
const CTRL_CHARS_RE = /[\x00-\x1f\x7f]/g;
const WHITESPACE_RE = /\s+/g;
const ROLE_TAG_RE = /\[role=/gi;
const CQ_TAG_RE = /\[cq:/gi;
const BROADCAST_AT_RE = /@(everyone|all|全体成员)/gi;

export class ProtocolError extends Error {
  constructor(code) {
    super(code);
    this.name = "ProtocolError";
    this.code = code;
  }
}

function unquoteEnvValue(raw) {
  if (typeof raw !== "string") return "";
  const trimmed = raw.trim();
  if (
    trimmed.length >= 2 &&
    ((trimmed.startsWith('"') && trimmed.endsWith('"')) ||
      (trimmed.startsWith("'") && trimmed.endsWith("'")))
  ) {
    return trimmed.slice(1, -1).trim();
  }
  return trimmed;
}

function parseBoolEnv(raw, defaultValue = false) {
  const val = unquoteEnvValue(raw).toLowerCase();
  if (!val) return defaultValue;
  if (val === "true" || val === "1" || val === "yes" || val === "on") return true;
  if (val === "false" || val === "0" || val === "no" || val === "off") return false;
  throw new ProtocolError("invalid_boolean_config");
}

function parseIntEnv(raw, defaultValue, min, max, errorCode) {
  const val = unquoteEnvValue(raw);
  if (!val) return defaultValue;
  if (!/^\d+$/.test(val)) {
    throw new ProtocolError(errorCode);
  }
  const num = Number.parseInt(val, 10);
  if (!Number.isSafeInteger(num) || num < min || num > max) {
    throw new ProtocolError(errorCode);
  }
  return num;
}

export function isValidBridgeToken(token) {
  if (typeof token !== "string" || token.length < 32) return false;
  if (/^(?:GENERATE_|REPLACE_)/i.test(token)) return false;
  return /^[!-~]+$/.test(token);
}

export function loadConfig(env = process.env) {
  const bridgeToken = unquoteEnvValue(env.MC_BRIDGE_TOKEN ?? "");
  if (!isValidBridgeToken(bridgeToken)) {
    throw new ProtocolError("invalid_mc_bridge_token");
  }

  const bridgeHost = unquoteEnvValue(env.MC_BRIDGE_HOST ?? "") || "0.0.0.0";
  const bridgePort = parseIntEnv(
    env.MC_BRIDGE_PORT,
    8791,
    0,
    65535,
    "invalid_mc_bridge_port",
  );

  const mcEnabled = parseBoolEnv(env.MC_ENABLED, false);
  const mcHost = unquoteEnvValue(env.MC_HOST ?? "");
  const mcPort = parseIntEnv(env.MC_PORT, 25565, 1, 65535, "invalid_mc_port");
  const mcUsername = unquoteEnvValue(env.MC_USERNAME ?? "") || "HakimiBot";
  const mcAuth = (unquoteEnvValue(env.MC_AUTH ?? "") || "offline").toLowerCase();
  if (mcAuth !== "offline" && mcAuth !== "microsoft") {
    throw new ProtocolError("invalid_mc_auth");
  }
  const mcVersion = unquoteEnvValue(env.MC_VERSION ?? "");
  const mcViewDistance = parseIntEnv(
    env.MC_VIEW_DISTANCE,
    4,
    2,
    16,
    "invalid_mc_view_distance",
  );
  const rawAllowed = unquoteEnvValue(env.MC_ALLOWED_PLAYERS ?? "");
  const mcAllowedPlayerList = rawAllowed
    ? rawAllowed
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : [];
  const mcAllowedPlayers = new Set(mcAllowedPlayerList.map((s) => s.toLowerCase()));
  const mcAllowChat = parseBoolEnv(env.MC_ALLOW_CHAT, false);

  return Object.freeze({
    bridgeHost,
    bridgePort,
    bridgeToken,
    mcEnabled,
    mcConfigured: mcHost.length > 0,
    mcHost,
    mcPort,
    mcUsername,
    mcAuth,
    mcVersion,
    mcViewDistance,
    mcAllowedPlayerList,
    mcAllowedPlayers,
    mcAllowChat,
  });
}

function sha256Digest(value) {
  return crypto.createHash("sha256").update(value, "utf8").digest();
}

export function verifyBearerToken(authHeader, expectedToken) {
  if (typeof authHeader !== "string" || typeof expectedToken !== "string" || !expectedToken) {
    return false;
  }
  const match = /^Bearer\s+([!-~]+)$/i.exec(authHeader.trim());
  const provided = match ? match[1] : "";
  const providedDigest = sha256Digest(provided);
  const expectedDigest = sha256Digest(expectedToken);
  const digestsEqual = crypto.timingSafeEqual(providedDigest, expectedDigest);
  return Boolean(match) && digestsEqual && provided.length === expectedToken.length;
}

class StrictJsonParser {
  constructor(source) {
    this.src = source;
    this.len = source.length;
    this.pos = 0;
  }

  skipWhitespace() {
    while (this.pos < this.len) {
      const ch = this.src.charCodeAt(this.pos);
      if (ch === 0x20 || ch === 0x0a || ch === 0x0d || ch === 0x09) {
        this.pos += 1;
      } else {
        break;
      }
    }
  }

  parseRootObject() {
    this.skipWhitespace();
    if (this.pos >= this.len || this.src.charCodeAt(this.pos) !== 0x7b) {
      throw new ProtocolError("non_object_json");
    }
    const obj = this.parseObject(0);
    this.skipWhitespace();
    if (this.pos !== this.len) {
      throw new ProtocolError("invalid_json");
    }
    return obj;
  }

  parseValue(depth) {
    if (depth > MAX_JSON_DEPTH) {
      throw new ProtocolError("json_depth_exceeded");
    }
    this.skipWhitespace();
    if (this.pos >= this.len) {
      throw new ProtocolError("invalid_json");
    }
    const ch = this.src.charCodeAt(this.pos);
    if (ch === 0x7b) return this.parseObject(depth + 1);
    if (ch === 0x5b) return this.parseArray(depth + 1);
    if (ch === 0x22) return this.parseString();
    if (ch === 0x74) return this.parseLiteral("true", true);
    if (ch === 0x66) return this.parseLiteral("false", false);
    if (ch === 0x6e) return this.parseLiteral("null", null);
    if (ch === 0x2d || (ch >= 0x30 && ch <= 0x39)) return this.parseNumber();
    throw new ProtocolError("invalid_json");
  }

  parseObject(depth) {
    if (depth > MAX_JSON_DEPTH) {
      throw new ProtocolError("json_depth_exceeded");
    }
    this.pos += 1; // consume '{'
    this.skipWhitespace();
    const obj = {};
    if (this.pos < this.len && this.src.charCodeAt(this.pos) === 0x7d) {
      this.pos += 1;
      return obj;
    }
    while (true) {
      this.skipWhitespace();
      if (this.pos >= this.len || this.src.charCodeAt(this.pos) !== 0x22) {
        throw new ProtocolError("invalid_json");
      }
      const key = this.parseString();
      if (Object.hasOwn(obj, key)) {
        throw new ProtocolError("duplicate_json_key");
      }
      this.skipWhitespace();
      if (this.pos >= this.len || this.src.charCodeAt(this.pos) !== 0x3a) {
        throw new ProtocolError("invalid_json");
      }
      this.pos += 1; // consume ':'
      const val = this.parseValue(depth);
      Object.defineProperty(obj, key, {
        value: val,
        writable: true,
        enumerable: true,
        configurable: true,
      });
      this.skipWhitespace();
      if (this.pos >= this.len) {
        throw new ProtocolError("invalid_json");
      }
      const sep = this.src.charCodeAt(this.pos);
      if (sep === 0x7d) {
        this.pos += 1;
        return obj;
      }
      if (sep === 0x2c) {
        this.pos += 1;
        continue;
      }
      throw new ProtocolError("invalid_json");
    }
  }

  parseArray(depth) {
    if (depth > MAX_JSON_DEPTH) {
      throw new ProtocolError("json_depth_exceeded");
    }
    this.pos += 1; // consume '['
    this.skipWhitespace();
    const arr = [];
    if (this.pos < this.len && this.src.charCodeAt(this.pos) === 0x5d) {
      this.pos += 1;
      return arr;
    }
    while (true) {
      arr.push(this.parseValue(depth));
      this.skipWhitespace();
      if (this.pos >= this.len) {
        throw new ProtocolError("invalid_json");
      }
      const sep = this.src.charCodeAt(this.pos);
      if (sep === 0x5d) {
        this.pos += 1;
        return arr;
      }
      if (sep === 0x2c) {
        this.pos += 1;
        continue;
      }
      throw new ProtocolError("invalid_json");
    }
  }

  parseString() {
    const start = this.pos;
    this.pos += 1; // consume opening '"'
    while (this.pos < this.len) {
      const ch = this.src.charCodeAt(this.pos);
      if (ch === 0x22) {
        this.pos += 1;
        try {
          return JSON.parse(this.src.slice(start, this.pos));
        } catch {
          throw new ProtocolError("invalid_json");
        }
      }
      if (ch === 0x5c) {
        this.pos += 2;
        continue;
      }
      if (ch < 0x20) {
        throw new ProtocolError("invalid_json");
      }
      this.pos += 1;
    }
    throw new ProtocolError("invalid_json");
  }

  parseLiteral(literal, value) {
    if (this.src.startsWith(literal, this.pos)) {
      this.pos += literal.length;
      return value;
    }
    throw new ProtocolError("invalid_json");
  }

  parseNumber() {
    const start = this.pos;
    if (this.src.charCodeAt(this.pos) === 0x2d) {
      this.pos += 1;
    }
    if (this.pos >= this.len) throw new ProtocolError("invalid_json");
    if (this.src.charCodeAt(this.pos) === 0x30) {
      this.pos += 1;
    } else if (
      this.src.charCodeAt(this.pos) >= 0x31 &&
      this.src.charCodeAt(this.pos) <= 0x39
    ) {
      while (
        this.pos < this.len &&
        this.src.charCodeAt(this.pos) >= 0x30 &&
        this.src.charCodeAt(this.pos) <= 0x39
      ) {
        this.pos += 1;
      }
    } else {
      throw new ProtocolError("invalid_json");
    }

    if (this.pos < this.len && this.src.charCodeAt(this.pos) === 0x2e) {
      this.pos += 1;
      if (
        this.pos >= this.len ||
        this.src.charCodeAt(this.pos) < 0x30 ||
        this.src.charCodeAt(this.pos) > 0x39
      ) {
        throw new ProtocolError("invalid_json");
      }
      while (
        this.pos < this.len &&
        this.src.charCodeAt(this.pos) >= 0x30 &&
        this.src.charCodeAt(this.pos) <= 0x39
      ) {
        this.pos += 1;
      }
    }

    if (
      this.pos < this.len &&
      (this.src.charCodeAt(this.pos) === 0x65 || this.src.charCodeAt(this.pos) === 0x45)
    ) {
      this.pos += 1;
      if (
        this.pos < this.len &&
        (this.src.charCodeAt(this.pos) === 0x2b || this.src.charCodeAt(this.pos) === 0x2d)
      ) {
        this.pos += 1;
      }
      if (
        this.pos >= this.len ||
        this.src.charCodeAt(this.pos) < 0x30 ||
        this.src.charCodeAt(this.pos) > 0x39
      ) {
        throw new ProtocolError("invalid_json");
      }
      while (
        this.pos < this.len &&
        this.src.charCodeAt(this.pos) >= 0x30 &&
        this.src.charCodeAt(this.pos) <= 0x39
      ) {
        this.pos += 1;
      }
    }

    const num = Number(this.src.slice(start, this.pos));
    if (!Number.isFinite(num)) {
      throw new ProtocolError("invalid_json");
    }
    return num;
  }
}

export function parseStrictJsonObject(raw, maxBytes = MAX_WS_PAYLOAD_BYTES) {
  let text;
  if (typeof raw === "string") {
    if (Buffer.byteLength(raw, "utf8") > maxBytes) {
      throw new ProtocolError("frame_too_large");
    }
    text = raw;
  } else if (Buffer.isBuffer(raw) || raw instanceof Uint8Array) {
    if (raw.byteLength > maxBytes) {
      throw new ProtocolError("frame_too_large");
    }
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(raw);
    } catch {
      throw new ProtocolError("invalid_json");
    }
  } else if (raw instanceof ArrayBuffer) {
    if (raw.byteLength > maxBytes) {
      throw new ProtocolError("frame_too_large");
    }
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(raw);
    } catch {
      throw new ProtocolError("invalid_json");
    }
  } else {
    throw new ProtocolError("invalid_json");
  }

  return new StrictJsonParser(text).parseRootObject();
}

export function hasForbiddenTargetOverride(value, depth = 0, seen = new Set()) {
  if (value === null || typeof value !== "object" || depth > 32) {
    return false;
  }
  if (seen.has(value)) return false;
  seen.add(value);

  if (Array.isArray(value)) {
    for (const item of value) {
      if (hasForbiddenTargetOverride(item, depth + 1, seen)) {
        return true;
      }
    }
    return false;
  }

  for (const [key, nested] of Object.entries(value)) {
    if (FORBIDDEN_TARGET_KEYS.has(key.toLowerCase())) {
      return true;
    }
    if (hasForbiddenTargetOverride(nested, depth + 1, seen)) {
      return true;
    }
  }
  return false;
}

export function sanitizeMcText(text, maxLen = 120) {
  if (text === null || text === undefined || maxLen <= 0) return "";
  let clean = String(text)
    .replace(ANSI_CSI_RE, " ")
    .replace(/[\r\n\t]+/g, " ")
    .replace(CTRL_CHARS_RE, "")
    .replace(ROLE_TAG_RE, "[untrusted_role=")
    .replace(CQ_TAG_RE, "［CQ:")
    .replace(BROADCAST_AT_RE, "＠$1")
    .replace(WHITESPACE_RE, " ")
    .trim();
  if (clean.length > maxLen) {
    clean = clean.slice(0, maxLen).trim();
  }
  return clean;
}

export class ChatRingBuffer {
  constructor(maxItems = 3, maxTotalChars = 120) {
    this.maxItems = maxItems;
    this.maxTotalChars = maxTotalChars;
    this.items = [];
  }

  push(rawText) {
    const clean = sanitizeMcText(rawText, this.maxTotalChars);
    if (!clean) return;
    this.items.push(clean);
    while (this.items.length > this.maxItems) {
      this.items.shift();
    }
  }

  clear() {
    this.items.length = 0;
  }

  format() {
    if (this.items.length === 0) return "";
    const sep = " / ";
    let list = [...this.items];
    let joined = list.join(sep);
    if (joined.length <= this.maxTotalChars) {
      return joined;
    }
    while (list.length > 1) {
      const budgetEach = Math.floor(
        (this.maxTotalChars - sep.length * (list.length - 1)) / list.length,
      );
      if (budgetEach >= 16) {
        const olderCount = list.length - 1;
        const latest = list[olderCount];
        const latestMax = Math.min(
          latest.length,
          Math.max(budgetEach, this.maxTotalChars - olderCount * (16 + sep.length)),
        );
        const rem = this.maxTotalChars - latestMax - sep.length * olderCount;
        const perOlder = Math.floor(rem / olderCount);
        const compacted = [
          ...list.slice(0, olderCount).map((item) => item.slice(0, perOlder).trim()),
          latest.slice(0, latestMax).trim(),
        ].filter(Boolean);
        joined = compacted.join(sep);
        if (joined.length <= this.maxTotalChars) {
          return joined;
        }
      }
      list.shift();
    }
    return list[0].slice(0, this.maxTotalChars).trim();
  }
}
