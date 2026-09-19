import http from "node:http";
import { randomUUID, randomBytes } from "node:crypto";
import { spawn } from "node:child_process";

const PUBLIC_PORT = Number(process.env.PORT || 10000);
const MCP_PORT = 18770;
const TUNNEL_HEALTH_PORT = 18771;
const FIXED_TUNNEL_ID = process.env.FIXED_TUNNEL_ID || "";
const MAX_BODY = 6 * 1024 * 1024;
const LINK_STALE_MS = 45_000;
const WORK_TIMEOUT_MS = 120_000;
const AGENTS = new Set(["pc", "android"]);

let tunnelChild = null;
let tunnelRuntimeKey = null;
const links = new Map();

function json(res, status, body, headers = {}) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(payload),
    ...headers,
  });
  res.end(payload);
}

function empty(res, status = 204) {
  res.writeHead(status);
  res.end();
}

async function readJson(req) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY) throw new Error("body_too_large");
    chunks.push(chunk);
  }
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function newLink(agentId, initResult, toolsListResult) {
  return {
    agentId,
    linkToken: randomBytes(32).toString("base64url"),
    initResult,
    toolsListResult,
    lastLinkAt: Date.now(),
    workQueue: [],
    workPending: new Map(),
    pollWaiters: [],
  };
}

function linkFresh(link) {
  return Boolean(link && Date.now() - link.lastLinkAt < LINK_STALE_MS);
}

function linkByToken(req) {
  const value = req.headers.authorization || "";
  if (!value.startsWith("Bearer ")) return null;
  const token = value.slice(7);
  for (const link of links.values()) {
    if (link.linkToken === token) return link;
  }
  return null;
}

function clearAgentLink(agentId, reason = "link_closed") {
  const link = links.get(agentId);
  if (!link) return;
  links.delete(agentId);

  while (link.pollWaiters.length) {
    const waiter = link.pollWaiters.shift();
    clearTimeout(waiter.timer);
    try { empty(waiter.res, 401); } catch {}
  }

  while (link.workQueue.length) {
    const item = link.workQueue.shift();
    const pending = link.workPending.get(item.id);
    if (pending) {
      clearTimeout(pending.timer);
      pending.reject(new Error(reason));
      link.workPending.delete(item.id);
    }
  }

  for (const [id, pending] of link.workPending) {
    clearTimeout(pending.timer);
    pending.reject(new Error(reason));
    link.workPending.delete(id);
  }
}

function dispatchWork(link) {
  while (link.workQueue.length && link.pollWaiters.length) {
    const item = link.workQueue.shift();
    const waiter = link.pollWaiters.shift();
    clearTimeout(waiter.timer);
    link.lastLinkAt = Date.now();
    json(waiter.res, 200, item);
  }
}

function enqueueWork(agentId, method, params) {
  const link = links.get(agentId);
  if (!linkFresh(link)) return Promise.reject(new Error(`${agentId}_link_unavailable`));
  const id = randomUUID();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      link.workPending.delete(id);
      reject(new Error(`${agentId}_work_timeout`));
    }, WORK_TIMEOUT_MS);
    link.workPending.set(id, { resolve, reject, timer });
    link.workQueue.push({ id, method, params: params ?? {} });
    dispatchWork(link);
  });
}

function mergedTools() {
  const out = [];
  const pc = links.get("pc")?.toolsListResult?.tools;
  if (Array.isArray(pc)) out.push(...pc);

  const android = links.get("android")?.toolsListResult?.tools;
  if (Array.isArray(android)) {
    for (const tool of android) {
      out.push({
        ...tool,
        name: `android__${tool.name}`,
        description: `[Android] ${tool.description || tool.name}`,
      });
    }
  }
  return out;
}

function initializeResult() {
  return links.get("pc")?.initResult || links.get("android")?.initResult || null;
}

function killTunnel() {
  if (tunnelChild && !tunnelChild.killed) {
    try { tunnelChild.kill("SIGTERM"); } catch {}
  }
  tunnelChild = null;
}

async function validateRuntimeKey(tunnelId, key) {
  if (!tunnelId || tunnelId !== FIXED_TUNNEL_ID || !key) return false;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8_000);
  try {
    const response = await fetch(
      `https://api.openai.com/v1/tunnels/${encodeURIComponent(tunnelId)}`,
      {
        method: "GET",
        headers: {
          authorization: `Bearer ${key}`,
          accept: "application/json",
          "user-agent": "mcp-edge-relay-bootstrap/1.1",
          "x-tunnel-client-name": "mcp-edge-relay-bootstrap",
          "x-tunnel-client-version": "1.1.0",
        },
        signal: controller.signal,
      },
    );
    return response.status === 200;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

async function waitTunnelReady(timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`http://127.0.0.1:${TUNNEL_HEALTH_PORT}/readyz`, {
        signal: AbortSignal.timeout(800),
      });
      if (r.ok) return true;
    } catch {}
    await new Promise((r) => setTimeout(r, 250));
  }
  return false;
}

async function ensureTunnelClient(tunnelId, key) {
  if (tunnelChild && !tunnelChild.killed) return;
  tunnelRuntimeKey = key;
  const env = {
    ...process.env,
    CONTROL_PLANE_TUNNEL_ID: tunnelId,
    CONTROL_PLANE_API_KEY: key,
    MCP_SERVER_URL: `channel=main,url=http://127.0.0.1:${MCP_PORT}/mcp`,
    MCP_STARTUP_WAIT_TIMEOUT: "5s",
    MCP_CONNECTION_MAX_TTL: "24h",
    CONTROL_PLANE_POLL_CHANNELS: "main",
    CONTROL_PLANE_MAX_INFLIGHT_REQUESTS: "25",
    MCP_MAX_CONCURRENT_REQUESTS: "25",
    LOG_LEVEL: "warn",
    LOG_FILE: "/dev/null",
    ADMIN_UI_LOG_BUFFER_EVENTS: "1",
    HEALTH_LISTEN_ADDR: `127.0.0.1:${TUNNEL_HEALTH_PORT}`,
    HEALTH_URL_FILE: "/tmp/tunnel-health.url",
  };

  const child = spawn("/usr/local/bin/tunnel-client", ["run"], {
    env,
    stdio: "ignore",
  });
  tunnelChild = child;
  child.once("exit", () => {
    if (tunnelChild === child) tunnelChild = null;
  });

  if (!(await waitTunnelReady())) {
    killTunnel();
    tunnelRuntimeKey = null;
    throw new Error("tunnel_start_timeout");
  }
}

const mcpServer = http.createServer(async (req, res) => {
  if (req.url !== "/mcp") return empty(res, 404);
  if (req.method === "GET" || req.method === "DELETE") return empty(res, 405);
  if (req.method !== "POST") return empty(res, 405);

  let body;
  try { body = await readJson(req); }
  catch { return json(res, 400, { error: "invalid_json" }); }

  if (!body || Array.isArray(body) || body.jsonrpc !== "2.0") {
    return json(res, 400, { error: "invalid_jsonrpc" });
  }

  const method = body.method;
  const hasId = Object.prototype.hasOwnProperty.call(body, "id");

  if (method === "notifications/initialized") return empty(res, 202);
  if (!hasId) return empty(res, 202);

  if (method === "initialize") {
    const init = initializeResult();
    if (!init) {
      return json(res, 200, {
        jsonrpc: "2.0",
        id: body.id,
        error: { code: -32000, message: "No agent link ready" },
      });
    }
    return json(
      res,
      200,
      { jsonrpc: "2.0", id: body.id, result: init },
      { "mcp-session-id": `edge-${randomUUID()}` },
    );
  }

  if (method === "tools/list") {
    return json(res, 200, {
      jsonrpc: "2.0",
      id: body.id,
      result: { tools: mergedTools() },
    });
  }

  if (method === "ping") {
    return json(res, 200, { jsonrpc: "2.0", id: body.id, result: {} });
  }

  let agentId = "pc";
  let params = body.params ?? {};
  if (method === "tools/call" && typeof params?.name === "string" && params.name.startsWith("android__")) {
    agentId = "android";
    params = { ...params, name: params.name.slice("android__".length) };
  }

  try {
    const reply = await enqueueWork(agentId, method, params);
    if (reply?.error) {
      return json(res, 200, { jsonrpc: "2.0", id: body.id, error: reply.error });
    }
    return json(res, 200, { jsonrpc: "2.0", id: body.id, result: reply?.result ?? {} });
  } catch (error) {
    return json(res, 200, {
      jsonrpc: "2.0",
      id: body.id,
      error: { code: -32603, message: String(error?.message || error) },
    });
  }
});

mcpServer.listen(MCP_PORT, "127.0.0.1");

const publicServer = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", "http://localhost");

  if (req.method === "GET" && url.pathname === "/health") {
    const agents = {};
    for (const id of AGENTS) {
      const link = links.get(id);
      agents[id] = {
        connected: Boolean(link),
        fresh: linkFresh(link),
        queued_work: link?.workQueue.length || 0,
        pending_work: link?.workPending.size || 0,
        tools: Array.isArray(link?.toolsListResult?.tools) ? link.toolsListResult.tools.length : 0,
      };
    }
    return json(res, 200, {
      status: "ok",
      tunnel_client_running: Boolean(tunnelChild && !tunnelChild.killed),
      agents,
      exposed_tools: mergedTools().length,
    });
  }

  if (req.method === "POST" && url.pathname === "/bootstrap") {
    let body;
    try { body = await readJson(req); }
    catch { return json(res, 400, { error: "invalid_json" }); }

    const agentId = typeof body.agentId === "string" ? body.agentId : "pc";
    if (!AGENTS.has(agentId)) return json(res, 400, { error: "invalid_agent_id" });
    if (
      typeof body.runtimeKey !== "string" ||
      typeof body.tunnelId !== "string" ||
      !body.initializeResult ||
      !body.toolsListResult
    ) {
      return json(res, 400, { error: "missing_bootstrap_fields" });
    }

    if (!(await validateRuntimeKey(body.tunnelId, body.runtimeKey))) {
      return json(res, 403, { error: "invalid_tunnel_credentials" });
    }

    try {
      await ensureTunnelClient(body.tunnelId, body.runtimeKey);
    } catch {
      return json(res, 503, { error: "tunnel_start_failed" });
    }

    clearAgentLink(agentId, "rebootstrap");
    const link = newLink(agentId, body.initializeResult, body.toolsListResult);
    links.set(agentId, link);

    return json(res, 200, {
      ok: true,
      agentId,
      linkToken: link.linkToken,
      tools: Array.isArray(body.toolsListResult?.tools) ? body.toolsListResult.tools.length : 0,
      exposedTools: mergedTools().length,
    });
  }

  if (req.method === "POST" && url.pathname === "/link/poll") {
    const link = linkByToken(req);
    if (!link) return empty(res, 401);
    link.lastLinkAt = Date.now();

    if (link.workQueue.length) {
      const item = link.workQueue.shift();
      return json(res, 200, item);
    }

    const waiter = { res, timer: null };
    waiter.timer = setTimeout(() => {
      const index = link.pollWaiters.indexOf(waiter);
      if (index >= 0) link.pollWaiters.splice(index, 1);
      link.lastLinkAt = Date.now();
      empty(res, 204);
    }, 15_000);
    link.pollWaiters.push(waiter);
    return;
  }

  if (req.method === "POST" && url.pathname === "/link/response") {
    const link = linkByToken(req);
    if (!link) return empty(res, 401);
    link.lastLinkAt = Date.now();
    let body;
    try { body = await readJson(req); }
    catch { return json(res, 400, { error: "invalid_json" }); }

    const pending = link.workPending.get(body.id);
    if (!pending) return empty(res, 404);
    link.workPending.delete(body.id);
    clearTimeout(pending.timer);
    pending.resolve({ result: body.result, error: body.error });
    return empty(res, 204);
  }

  if (req.method === "POST" && url.pathname === "/link/disconnect") {
    const link = linkByToken(req);
    if (!link) return empty(res, 401);
    clearAgentLink(link.agentId, `${link.agentId}_disconnect`);
    return empty(res, 204);
  }

  return empty(res, 404);
});

setInterval(() => {
  for (const [agentId, link] of links) {
    if (Date.now() - link.lastLinkAt > LINK_STALE_MS) {
      clearAgentLink(agentId, `${agentId}_link_stale`);
    }
  }
}, 5_000).unref();

function shutdown() {
  for (const agentId of [...links.keys()]) clearAgentLink(agentId, "edge_shutdown");
  killTunnel();
  tunnelRuntimeKey = null;
  publicServer.close();
  mcpServer.close();
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

publicServer.listen(PUBLIC_PORT, "0.0.0.0");
