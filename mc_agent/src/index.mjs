import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocket, WebSocketServer } from "ws";
import { McRuntime } from "./behaviors.mjs";
import {
  BRIDGE_VERSION,
  MAX_WS_PAYLOAD_BYTES,
  ProtocolError,
  loadConfig,
  parseStrictJsonObject,
  verifyBearerToken,
} from "./protocol.mjs";

export function createMcBridgeServer(options = {}) {
  const config = options.config ?? loadConfig(options.env ?? process.env);
  const clients = new Set();

  function sendJson(ws, payload) {
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify(payload));
  }

  function broadcastEvent(eventName, payload) {
    const frame = {
      type: "event",
      event: eventName,
      ts: Date.now() / 1000,
      payload,
    };
    for (const ws of clients) {
      sendJson(ws, frame);
    }
  }

  const runtime = new McRuntime({
    config,
    createBotFn: options.createBotFn,
    onEvent: broadcastEvent,
    snapshotIntervalMs: options.snapshotIntervalMs,
    milestoneCooldownMs: options.milestoneCooldownMs,
    chatCooldownMs: options.chatCooldownMs,
    actionTimeoutMs: options.actionTimeoutMs,
    profilesFolder: options.profilesFolder,
    nowFn: options.nowFn,
    fetchFn: options.fetchFn,
    powerPollIntervalMs: options.powerPollIntervalMs,
    powerWakeTimeoutMs: options.powerWakeTimeoutMs,
    idleDisconnectMs: options.idleDisconnectMs,
    companionTickMs: options.companionTickMs,
    soloWanderIntervalMs: options.soloWanderIntervalMs,
    companionReplyCooldownMs: options.companionReplyCooldownMs,
    hostileAlertCooldownMs: options.hostileAlertCooldownMs,
  });

  const httpServer = http.createServer((req, res) => {
    let url;
    try {
      url = new URL(req.url || "/", "http://localhost");
    } catch {
      res.writeHead(400, { "Content-Type": "application/json; charset=utf-8" });
      res.end('{"error":"bad_request"}');
      return;
    }

    if (req.method === "GET" && url.pathname === "/healthz") {
      res.writeHead(200, {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
      });
      res.end(JSON.stringify(runtime.getHealthz()));
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json; charset=utf-8" });
    res.end('{"error":"not_found"}');
  });

  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: MAX_WS_PAYLOAD_BYTES,
  });

  httpServer.on("upgrade", (req, socket, head) => {
    let url;
    try {
      url = new URL(req.url || "/", "http://localhost");
    } catch {
      socket.write("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }

    if (url.pathname !== "/ws" || url.search !== "") {
      socket.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }

    if (!verifyBearerToken(req.headers.authorization, config.bridgeToken)) {
      console.warn("event=mc_ws_auth_rejected");
      socket.write(
        "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer\r\nConnection: close\r\n\r\n",
      );
      socket.destroy();
      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  });

  wss.on("connection", (ws) => {
    clients.add(ws);
    ws.on("close", () => clients.delete(ws));
    ws.on("error", () => clients.delete(ws));

    const initialSnap = runtime.getSnapshot();
    const ts = Date.now() / 1000;
    sendJson(ws, {
      type: "event",
      event: "mc.hello",
      ts,
      payload: {
        version: BRIDGE_VERSION,
        bridge_ready: true,
        mc_enabled: config.mcEnabled,
        mc_configured: config.mcConfigured,
        paused: runtime.paused,
        snapshot: initialSnap,
      },
    });
    sendJson(ws, {
      type: "event",
      event: "mc.snapshot",
      ts,
      payload: initialSnap,
    });

    ws.on("message", async (raw) => {
      let frame;
      try {
        frame = parseStrictJsonObject(raw, MAX_WS_PAYLOAD_BYTES);
      } catch (err) {
        const code = err instanceof ProtocolError ? err.code : "invalid_json";
        console.warn(`event=mc_ws_bad_frame err=${code}`);
        try {
          ws.close(1008, code);
        } catch {
          ws.terminate();
        }
        return;
      }

      if (frame.type !== "rpc_request") {
        return;
      }

      const reqId = frame.id;
      const action = frame.action;
      if (
        typeof reqId !== "string" ||
        reqId.length === 0 ||
        reqId.length > 128 ||
        typeof action !== "string" ||
        action.length === 0
      ) {
        if (typeof reqId === "string" && reqId.length > 0 && reqId.length <= 128) {
          sendJson(ws, {
            type: "rpc_response",
            id: reqId,
            ok: false,
            data: {},
            error: "invalid_rpc_frame",
          });
        }
        return;
      }

      try {
        const data = await runtime.handleRpc(action, frame.params);
        sendJson(ws, {
          type: "rpc_response",
          id: reqId,
          ok: true,
          data,
          error: null,
        });
      } catch (err) {
        if (err instanceof ProtocolError) {
          sendJson(ws, {
            type: "rpc_response",
            id: reqId,
            ok: false,
            data: {},
            error: err.code,
          });
          return;
        }
        console.warn(`event=mc_rpc_internal_error action=${action}`);
        sendJson(ws, {
          type: "rpc_response",
          id: reqId,
          ok: false,
          data: {},
          error: "mc_internal_error",
        });
      }
    });
  });

  return {
    config,
    runtime,
    httpServer,
    wss,
    async start() {
      await new Promise((resolve, reject) => {
        const onErr = (err) => reject(err);
        httpServer.once("error", onErr);
        httpServer.listen(config.bridgePort, config.bridgeHost, () => {
          httpServer.off("error", onErr);
          resolve();
        });
      });
      runtime.start();
      const addr = httpServer.address();
      return {
        host: typeof addr === "object" && addr ? addr.address : config.bridgeHost,
        port: typeof addr === "object" && addr ? addr.port : config.bridgePort,
      };
    },
    async close() {
      runtime.stop();
      for (const ws of clients) {
        try {
          ws.close(1001, "server_shutdown");
        } catch {
          ws.terminate();
        }
      }
      clients.clear();
      await new Promise((resolve) => wss.close(() => resolve()));
      await new Promise((resolve) => {
        if (!httpServer.listening) {
          resolve();
          return;
        }
        httpServer.close(() => resolve());
      });
    },
  };
}

function isMainModule() {
  if (!process.argv[1]) return false;
  return path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
}

if (isMainModule()) {
  let server;
  try {
    server = createMcBridgeServer();
    const addr = await server.start();
    console.info(`event=mc_bridge_listening host=${addr.host} port=${addr.port}`);
  } catch (err) {
    const code = err instanceof ProtocolError ? err.code : "startup_error";
    console.error(`event=mc_bridge_startup_failed err=${code}`);
    process.exit(1);
  }

  let closing = false;
  const shutdown = async () => {
    if (closing) return;
    closing = true;
    try {
      await server.close();
    } finally {
      process.exit(0);
    }
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
