import childProcess from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import express from "express";
import httpProxy from "http-proxy";
import pty from "node-pty";
import { WebSocketServer } from "ws";

const PORT = Number.parseInt(process.env.PORT ?? "8080", 10);
const STATE_DIR =
  process.env.OPENCLAW_STATE_DIR?.trim() ||
  path.join(os.homedir(), ".openclaw");
const WORKSPACE_DIR =
  process.env.OPENCLAW_WORKSPACE_DIR?.trim() ||
  path.join(STATE_DIR, "workspace");

const SETUP_PASSWORD = process.env.SETUP_PASSWORD?.trim();

function resolveGatewayToken() {
  const envTok = process.env.OPENCLAW_GATEWAY_TOKEN?.trim();
  if (envTok) return envTok;

  const tokenPath = path.join(STATE_DIR, "gateway.token");
  try {
    const existing = fs.readFileSync(tokenPath, "utf8").trim();
    if (existing) return existing;
  } catch (err) {
    console.warn(
      `[gateway-token] could not read existing token: ${err.code || err.message}`,
    );
  }

  const generated = crypto.randomBytes(32).toString("hex");
  try {
    fs.mkdirSync(STATE_DIR, { recursive: true });
    fs.writeFileSync(tokenPath, generated, { encoding: "utf8", mode: 0o600 });
  } catch (err) {
    console.warn(
      `[gateway-token] could not persist token: ${err.code || err.message}`,
    );
  }
  return generated;
}

const OPENCLAW_GATEWAY_TOKEN = resolveGatewayToken();
process.env.OPENCLAW_GATEWAY_TOKEN = OPENCLAW_GATEWAY_TOKEN;

let cachedOpenclawVersion = null;
let cachedChannelsHelp = null;

async function getOpenclawInfo() {
  if (!cachedOpenclawVersion) {
    const [version, channelsHelp] = await Promise.all([
      runCmd(OPENCLAW_NODE, clawArgs(["--version"])),
      runCmd(OPENCLAW_NODE, clawArgs(["channels", "add", "--help"])),
    ]);
    cachedOpenclawVersion = version.output.trim();
    cachedChannelsHelp = channelsHelp.output;
  }
  return { version: cachedOpenclawVersion, channelsHelp: cachedChannelsHelp };
}

const INTERNAL_GATEWAY_PORT = Number.parseInt(
  process.env.INTERNAL_GATEWAY_PORT ?? "18789",
  10,
);
const INTERNAL_GATEWAY_HOST = process.env.INTERNAL_GATEWAY_HOST ?? "127.0.0.1";
const GATEWAY_TARGET = `http://${INTERNAL_GATEWAY_HOST}:${INTERNAL_GATEWAY_PORT}`;

const OPENCLAW_ENTRY =
  process.env.OPENCLAW_ENTRY?.trim() || "/openclaw/dist/entry.js";
const OPENCLAW_NODE = process.env.OPENCLAW_NODE?.trim() || "node";

const ENABLE_WEB_TUI = process.env.ENABLE_WEB_TUI?.toLowerCase() === "true";
const TUI_IDLE_TIMEOUT_MS = Number.parseInt(
  process.env.TUI_IDLE_TIMEOUT_MS ?? "300000",
  10,
);
const TUI_MAX_SESSION_MS = Number.parseInt(
  process.env.TUI_MAX_SESSION_MS ?? "1800000",
  10,
);
const CUSTOM_PROVIDER_BOOTSTRAP_OPENAI_KEY =
  "sk-placeholder-for-custom-provider";

const AI_PROVIDER = process.env.AI_PROVIDER?.trim() || "";
const PROVIDER_BASE_URL = process.env.PROVIDER_BASE_URL?.trim() || "";
const PROVIDER_API_KEY = process.env.PROVIDER_API_KEY?.trim() || "";

const VALID_ENV_AI_PROVIDERS = [
  "openai",
  "anthropic",
  "google",
  "openrouter",
  "ai-gateway",
  "moonshot",
  "zai",
  "minimax",
  "synthetic",
  "opencode-zen",
  "volcengine-plan",
  "bedrock",
  "bailian",
  "ollama",
  "custom-provider",
];

function clawArgs(args) {
  return [OPENCLAW_ENTRY, ...args];
}

function configPath() {
  return (
    process.env.OPENCLAW_CONFIG_PATH?.trim() ||
    path.join(STATE_DIR, "openclaw.json")
  );
}

function isConfigured() {
  try {
    return fs.existsSync(configPath());
  } catch {
    return false;
  }
}

function inferCustomApiTypeByProvider(providerName) {
  const p = String(providerName || "").trim().toLowerCase();
  if (p === "anthropic" || p === "bedrock") return "anthropic-messages";
  return "openai-completions";
}

function validateEnvProviderConfig() {
  const enabled = Boolean(AI_PROVIDER);
  const values = {
    aiProvider: AI_PROVIDER,
    providerBaseUrl: PROVIDER_BASE_URL,
    providerApiKey: PROVIDER_API_KEY,
  };

  if (!enabled) {
    return {
      enabled: false,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      error: null,
    };
  }

  if (!VALID_ENV_AI_PROVIDERS.includes(AI_PROVIDER)) {
    return {
      enabled: true,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      error:
        `Invalid AI_PROVIDER: ${AI_PROVIDER}. ` +
        `Allowed values: ${VALID_ENV_AI_PROVIDERS.join(", ")}. ` +
        "Please update Railway Variables (AI_PROVIDER / PROVIDER_BASE_URL / PROVIDER_API_KEY).",
    };
  }

  if (!PROVIDER_BASE_URL) {
    return {
      enabled: true,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      error:
        "PROVIDER_BASE_URL is required when AI_PROVIDER is set. " +
        "Please update Railway Variables.",
    };
  }

  try {
    const u = new URL(PROVIDER_BASE_URL);
    if (u.protocol !== "http:" && u.protocol !== "https:") {
      throw new Error("invalid-protocol");
    }
  } catch {
    return {
      enabled: true,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      error:
        `Invalid PROVIDER_BASE_URL: ${PROVIDER_BASE_URL}. ` +
        "Expected an absolute URL starting with http:// or https://. " +
        "Please update Railway Variables.",
    };
  }

  if (!PROVIDER_API_KEY) {
    return {
      enabled: true,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      error:
        "PROVIDER_API_KEY is required when AI_PROVIDER is set. " +
        "Please update Railway Variables.",
    };
  }

  return {
    enabled: true,
    valid: true,
    values,
    allowedProviders: VALID_ENV_AI_PROVIDERS,
    error: null,
  };
}

function buildPayloadFromEnvProvider(rawPayload = {}) {
  const envValidation = validateEnvProviderConfig();
  if (!envValidation.enabled) {
    return {
      ok: false,
      error:
        "ProviderFromEnv was selected, but AI_PROVIDER is empty. " +
        "Please set Railway Variables AI_PROVIDER / PROVIDER_BASE_URL / PROVIDER_API_KEY.",
    };
  }
  if (!envValidation.valid) {
    return {
      ok: false,
      error: envValidation.error,
    };
  }

  const rawModel = typeof rawPayload.model === "string" ? rawPayload.model.trim() : "";
  let customModelId = "";
  if (rawModel) {
    const prefixed = `${AI_PROVIDER}/`;
    customModelId = rawModel.startsWith(prefixed) ? rawModel.slice(prefixed.length) : rawModel;
  }

  return {
    ok: true,
    payload: {
      ...rawPayload,
      authChoice: "custom-provider",
      authSecret: PROVIDER_API_KEY,
      customProviderName: AI_PROVIDER,
      customBaseUrl: PROVIDER_BASE_URL,
      customApiType: inferCustomApiTypeByProvider(AI_PROVIDER),
      customModelId,
    },
  };
}

let gatewayProc = null;
let gatewayStarting = null;
let shuttingDown = false;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForGatewayReady(opts = {}) {
  const timeoutMs = opts.timeoutMs ?? 60_000;
  const start = Date.now();
  const endpoints = ["/openclaw", "/openclaw", "/", "/health"];

  while (Date.now() - start < timeoutMs) {
    for (const endpoint of endpoints) {
      try {
        const res = await fetch(`${GATEWAY_TARGET}${endpoint}`, {
          method: "GET",
        });
        if (res) {
          console.log(`[gateway] ready at ${endpoint}`);
          return true;
        }
      } catch (err) {
        if (err.code !== "ECONNREFUSED" && err.cause?.code !== "ECONNREFUSED") {
          const msg = err.code || err.message;
          if (msg !== "fetch failed" && msg !== "UND_ERR_CONNECT_TIMEOUT") {
            console.warn(`[gateway] health check error: ${msg}`);
          }
        }
      }
    }
    await sleep(250);
  }
  console.error(`[gateway] failed to become ready after ${timeoutMs / 1000} seconds`);
  return false;
}

async function startGateway() {
  if (gatewayProc) return;
  if (!isConfigured()) throw new Error("Gateway cannot start: not configured");

  fs.mkdirSync(STATE_DIR, { recursive: true });
  fs.mkdirSync(WORKSPACE_DIR, { recursive: true });

  for (const lockPath of [
    path.join(STATE_DIR, "gateway.lock"),
    "/tmp/openclaw-gateway.lock",
  ]) {
    try {
      fs.rmSync(lockPath, { force: true });
    } catch {}
  }

  const args = [
    "gateway",
    "run",
    "--bind",
    "loopback",
    "--port",
    String(INTERNAL_GATEWAY_PORT),
    "--auth",
    "token",
    "--token",
    OPENCLAW_GATEWAY_TOKEN,
    "--allow-unconfigured",
  ];

  gatewayProc = childProcess.spawn(OPENCLAW_NODE, clawArgs(args), {
    stdio: "inherit",
    env: {
      ...process.env,
      OPENCLAW_STATE_DIR: STATE_DIR,
      OPENCLAW_WORKSPACE_DIR: WORKSPACE_DIR,
    },
  });

  const safeArgs = args.map((arg, i) =>
    args[i - 1] === "--token" ? "[REDACTED]" : arg
  );
  console.log(
    `[gateway] starting with command: ${OPENCLAW_NODE} ${clawArgs(safeArgs).join(" ")}`,
  );
  console.log(`[gateway] STATE_DIR: ${STATE_DIR}`);
  console.log(`[gateway] WORKSPACE_DIR: ${WORKSPACE_DIR}`);
  console.log(`[gateway] config path: ${configPath()}`);

  gatewayProc.on("error", (err) => {
    console.error(`[gateway] spawn error: ${String(err)}`);
    gatewayProc = null;
  });

  gatewayProc.on("exit", (code, signal) => {
    console.error(`[gateway] exited code=${code} signal=${signal}`);
    gatewayProc = null;
    if (!shuttingDown && isConfigured()) {
      console.log("[gateway] scheduling auto-restart in 2s...");
      setTimeout(() => {
        if (!shuttingDown && !gatewayProc && isConfigured()) {
          ensureGatewayRunning().catch((err) => {
            console.error(`[gateway] auto-restart failed: ${err.message}`);
          });
        }
      }, 2000);
    }
  });
}

async function ensureGatewayRunning() {
  if (!isConfigured()) return { ok: false, reason: "not configured" };
  if (gatewayProc) return { ok: true };
  if (!gatewayStarting) {
    gatewayStarting = (async () => {
      await startGateway();
      const ready = await waitForGatewayReady({ timeoutMs: 60_000 });
      if (!ready) {
        throw new Error("Gateway did not become ready in time");
      }
    })().finally(() => {
      gatewayStarting = null;
    });
  }
  await gatewayStarting;
  return { ok: true };
}

function isGatewayStarting() {
  return gatewayStarting !== null;
}

function isGatewayReady() {
  return gatewayProc !== null && gatewayStarting === null;
}

async function restartGateway() {
  if (gatewayProc) {
    try {
      gatewayProc.kill("SIGTERM");
    } catch (err) {
      console.warn(`[gateway] kill error: ${err.message}`);
    }
    await sleep(750);
    gatewayProc = null;
  }
  return ensureGatewayRunning();
}

const setupRateLimiter = {
  attempts: new Map(),
  windowMs: 60_000,
  maxAttempts: 50,
  cleanupInterval: setInterval(function () {
    const now = Date.now();
    for (const [ip, data] of setupRateLimiter.attempts) {
      if (now - data.windowStart > setupRateLimiter.windowMs) {
        setupRateLimiter.attempts.delete(ip);
      }
    }
  }, 60_000),

  isRateLimited(ip) {
    const now = Date.now();
    const data = this.attempts.get(ip);
    if (!data || now - data.windowStart > this.windowMs) {
      this.attempts.set(ip, { windowStart: now, count: 1 });
      return false;
    }
    data.count++;
    return data.count > this.maxAttempts;
  },
};

function requireSetupAuth(req, res, next) {
  if (!SETUP_PASSWORD) {
    return res
      .status(500)
      .type("text/plain")
      .send(
        "SETUP_PASSWORD is not set. Set it in Railway Variables before using /setup.",
      );
  }

  const ip = req.ip || req.socket?.remoteAddress || "unknown";
  if (setupRateLimiter.isRateLimited(ip)) {
    return res.status(429).type("text/plain").send("Too many requests. Try again later.");
  }

  const header = req.headers.authorization || "";
  const [scheme, encoded] = header.split(" ");
  if (scheme !== "Basic" || !encoded) {
    res.set("WWW-Authenticate", 'Basic realm="OpenClaw Setup"');
    return res.status(401).send("Auth required");
  }
  const decoded = Buffer.from(encoded, "base64").toString("utf8");
  const idx = decoded.indexOf(":");
  const password = idx >= 0 ? decoded.slice(idx + 1) : "";
  const passwordHash = crypto.createHash("sha256").update(password).digest();
  const expectedHash = crypto.createHash("sha256").update(SETUP_PASSWORD).digest();
  const isValid = crypto.timingSafeEqual(passwordHash, expectedHash);
  if (!isValid) {
    res.set("WWW-Authenticate", 'Basic realm="OpenClaw Setup"');
    return res.status(401).send("Invalid password");
  }
  return next();
}

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "1mb" }));

app.get("/healthz", async (_req, res) => {
  let gateway = "unconfigured";
  if (isConfigured()) {
    gateway = isGatewayReady() ? "ready" : "starting";
  }
  res.json({ ok: true, gateway });
});

app.get("/setup/healthz", async (_req, res) => {
  const configured = isConfigured();
  const gatewayRunning = isGatewayReady();
  const starting = isGatewayStarting();
  let gatewayReachable = false;

  if (gatewayRunning) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 3000);
      const r = await fetch(`${GATEWAY_TARGET}/`, { signal: controller.signal });
      clearTimeout(timeout);
      gatewayReachable = r !== null;
    } catch {}
  }

  res.json({
    ok: true,
    wrapper: true,
    configured,
    gatewayRunning,
    gatewayStarting: starting,
    gatewayReachable,
  });
});

app.get("/setup", requireSetupAuth, (_req, res) => {
  res.sendFile(path.join(process.cwd(), "src", "public", "setup.html"));
});

app.get("/setup/api/status", requireSetupAuth, async (_req, res) => {
  const { version, channelsHelp } = await getOpenclawInfo();
  const envProvider = validateEnvProviderConfig();

  const authGroups = [
    {
      value: "openai",
      label: "OpenAI",
      hint: "API key",
      options: [
        { value: "openai-api-key", label: "OpenAI API key" },
      ],
    },
    {
      value: "anthropic",
      label: "Anthropic",
      hint: "API key",
      options: [
        { value: "apiKey", label: "Anthropic API key" },
      ],
    },
    {
      value: "google",
      label: "Google",
      hint: "API key",
      options: [
        { value: "gemini-api-key", label: "Google Gemini API key" },
      ],
    },
    {
      value: "openrouter",
      label: "OpenRouter",
      hint: "API key",
      options: [{ value: "openrouter-api-key", label: "OpenRouter API key" }],
    },
    {
      value: "ai-gateway",
      label: "Vercel AI Gateway",
      hint: "API key",
      options: [
        { value: "ai-gateway-api-key", label: "Vercel AI Gateway API key" },
      ],
    },
    {
      value: "moonshot",
      label: "Moonshot AI",
      hint: "Kimi K2 + Kimi Code",
      options: [
        { value: "moonshot-api-key", label: "Moonshot AI API key" },
        { value: "kimi-code-api-key", label: "Kimi Code API key" },
      ],
    },
    {
      value: "zai",
      label: "Z.AI (GLM 4.7)",
      hint: "API key",
      options: [{ value: "zai-api-key", label: "Z.AI (GLM 4.7) API key" }],
    },
    {
      value: "minimax",
      label: "MiniMax",
      hint: "M2.1 (recommended)",
      options: [
        { value: "minimax-api", label: "MiniMax M2.1" },
        { value: "minimax-api-lightning", label: "MiniMax M2.1 Lightning" },
      ],
    },
    {
      value: "qwen",
      label: "Qwen",
      hint: "OAuth",
      options: [{ value: "qwen-portal", label: "Qwen OAuth" }],
    },
    {
      value: "copilot",
      label: "Copilot",
      hint: "GitHub + local proxy",
      options: [
        {
          value: "github-copilot",
          label: "GitHub Copilot (GitHub device login)",
        },
        { value: "copilot-proxy", label: "Copilot Proxy (local)" },
      ],
    },
    {
      value: "synthetic",
      label: "Synthetic",
      hint: "Anthropic-compatible (multi-model)",
      options: [{ value: "synthetic-api-key", label: "Synthetic API key" }],
    },
    {
      value: "opencode-zen",
      label: "OpenCode Zen",
      hint: "API key",
      options: [
        { value: "opencode-zen", label: "OpenCode Zen (multi-model proxy)" },
      ],
    },
    {
      value: "volcengine-plan",
      label: "VolcEngine Coding Plan (火山引擎)",
      hint: "Coding Plan API",
      custom: true,
      options: [
        { value: "volcengine-plan", label: "VolcEngine Coding Plan" },
      ],
      defaults: {
        customProviderName: "volcengine-plan",
        customBaseUrl: "https://ark.cn-beijing.volces.com/api/coding/v3",
        customApiType: "openai-completions",
        customModelId: "ark-code-latest",
      },
    },
    {
      value: "bedrock",
      label: "Amazon Bedrock",
      hint: "Anthropic via AWS",
      custom: true,
      options: [
        { value: "bedrock", label: "Amazon Bedrock" },
      ],
      defaults: {
        customProviderName: "bedrock",
        customBaseUrl: "",
        customApiType: "anthropic-messages",
        customModelId: "",
      },
    },
    {
      value: "bailian",
      label: "Alibaba Bailian (阿里百炼)",
      hint: "DashScope API",
      custom: true,
      options: [
        { value: "bailian", label: "Alibaba Bailian" },
      ],
      defaults: {
        customProviderName: "bailian",
        customBaseUrl: "https://dashscope.aliyuncs.com/compatible-mode/v1",
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "ollama",
      label: "Ollama (Local)",
      hint: "Local models, no API key needed",
      custom: true,
      options: [
        { value: "ollama", label: "Ollama Local" },
      ],
      defaults: {
        customProviderName: "ollama",
        customBaseUrl: "http://localhost:11434/v1",
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "custom-provider",
      label: "Custom Provider",
      hint: "OpenAI / Anthropic compatible",
      custom: true,
      options: [
        { value: "custom-provider", label: "Custom Provider" },
      ],
      defaults: {
        customProviderName: "",
        customBaseUrl: "",
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
  ];

  if (envProvider.enabled) {
    authGroups.unshift({
      value: "provider-from-env",
      label: "ProviderFromEnv",
      hint: envProvider.valid ? `AI_PROVIDER=${envProvider.values.aiProvider}` : "Invalid Railway Variables",
      custom: true,
      fromEnv: true,
      options: [
        {
          value: "provider-from-env",
          label: "Use AI_PROVIDER / PROVIDER_BASE_URL / PROVIDER_API_KEY",
        },
      ],
      defaults: {
        customProviderName: envProvider.values.aiProvider || "",
        customBaseUrl: envProvider.values.providerBaseUrl || "",
        customApiType: inferCustomApiTypeByProvider(envProvider.values.aiProvider),
        customModelId: "",
      },
    });
  }

  res.json({
    configured: isConfigured(),
    gatewayTarget: GATEWAY_TARGET,
    openclawVersion: version,
    channelsAddHelp: channelsHelp,
    authGroups,
    tuiEnabled: ENABLE_WEB_TUI,
    envProvider,
  });
});

function buildOnboardArgs(payload) {
  const args = [
    "onboard",
    "--non-interactive",
    "--accept-risk",
    "--json",
    "--no-install-daemon",
    "--skip-health",
    "--workspace",
    WORKSPACE_DIR,
    "--gateway-bind",
    "loopback",
    "--gateway-port",
    String(INTERNAL_GATEWAY_PORT),
    "--gateway-auth",
    "token",
    "--gateway-token",
    OPENCLAW_GATEWAY_TOKEN,
    "--flow",
    "quickstart",
  ];

  if (payload.authChoice) {
    if (isCustomProvider(payload.authChoice)) {
      // Custom providers: bootstrap with openai-api-key placeholder to create initial config
      args.push(
        "--auth-choice",
        "openai-api-key",
        "--openai-api-key",
        CUSTOM_PROVIDER_BOOTSTRAP_OPENAI_KEY,
      );
    } else {
      args.push("--auth-choice", payload.authChoice);

      const secret = (payload.authSecret || "").trim();
      const map = {
        "openai-api-key": "--openai-api-key",
        apiKey: "--anthropic-api-key",
        "openrouter-api-key": "--openrouter-api-key",
        "ai-gateway-api-key": "--ai-gateway-api-key",
        "moonshot-api-key": "--moonshot-api-key",
        "kimi-code-api-key": "--kimi-code-api-key",
        "gemini-api-key": "--gemini-api-key",
        "zai-api-key": "--zai-api-key",
        "minimax-api": "--minimax-api-key",
        "minimax-api-lightning": "--minimax-api-key",
        "synthetic-api-key": "--synthetic-api-key",
        "opencode-zen": "--opencode-zen-api-key",
      };
      const flag = map[payload.authChoice];
      if (flag && secret) {
        args.push(flag, secret);
      }
    }
  }

  return args;
}

function runCmd(cmd, args, opts = {}) {
  return new Promise((resolve) => {
    const proc = childProcess.spawn(cmd, args, {
      ...opts,
      env: {
        ...process.env,
        OPENCLAW_STATE_DIR: STATE_DIR,
        OPENCLAW_WORKSPACE_DIR: WORKSPACE_DIR,
      },
    });

    let out = "";
    proc.stdout?.on("data", (d) => (out += d.toString("utf8")));
    proc.stderr?.on("data", (d) => (out += d.toString("utf8")));

    proc.on("error", (err) => {
      out += `\n[spawn error] ${String(err)}\n`;
      resolve({ code: 127, output: out });
    });

    proc.on("close", (code) => resolve({ code: code ?? 0, output: out }));
  });
}

function parseJsonFromOutput(rawOutput) {
  if (!rawOutput || typeof rawOutput !== "string") return null;
  const trimmed = rawOutput.trim();
  if (!trimmed) return null;

  try {
    return JSON.parse(trimmed);
  } catch {}

  for (let start = 0; start < trimmed.length; start++) {
    const startChar = trimmed[start];
    if (startChar !== "{" && startChar !== "[") continue;

    let inString = false;
    let escape = false;
    const stack = [startChar];

    for (let end = start + 1; end < trimmed.length; end++) {
      const ch = trimmed[end];

      if (inString) {
        if (escape) {
          escape = false;
          continue;
        }
        if (ch === "\\") {
          escape = true;
          continue;
        }
        if (ch === "\"") {
          inString = false;
        }
        continue;
      }

      if (ch === "\"") {
        inString = true;
        continue;
      }

      if (ch === "{" || ch === "[") {
        stack.push(ch);
        continue;
      }

      if (ch !== "}" && ch !== "]") continue;
      const top = stack[stack.length - 1];
      const matches =
        (top === "{" && ch === "}") ||
        (top === "[" && ch === "]");
      if (!matches) {
        break;
      }
      stack.pop();
      if (stack.length !== 0) continue;

      const candidate = trimmed.slice(start, end + 1);
      try {
        return JSON.parse(candidate);
      } catch {
        break;
      }
    }
  }

  return null;
}

const PENDING_DEVICE_STATUSES = new Set([
  "pending",
  "requested",
  "requesting",
  "awaiting",
  "waiting",
  "unapproved",
]);

const APPROVED_DEVICE_STATUSES = new Set([
  "approved",
  "paired",
  "active",
  "connected",
]);

function looksLikeDeviceEntry(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  return (
    "requestId" in value ||
    "request_id" in value ||
    "deviceId" in value ||
    "device_id" in value ||
    (
      "id" in value &&
      ("status" in value || "state" in value || "createdAt" in value || "created_at" in value)
    )
  );
}

function normalizeDeviceEntry(value) {
  if (!value || typeof value !== "object") return null;
  const entry = { ...value };
  if (entry.requestId == null && entry.request_id != null) entry.requestId = entry.request_id;
  if (entry.deviceId == null && entry.device_id != null) entry.deviceId = entry.device_id;
  if (entry.createdAt == null && entry.created_at != null) entry.createdAt = entry.created_at;
  return entry;
}

function classifyDeviceContext(key) {
  const lower = String(key || "").toLowerCase();
  if (lower.includes("pending") || lower.includes("request")) return "pending";
  if (
    lower.includes("paired") ||
    lower.includes("approved") ||
    lower === "devices" ||
    lower.includes("device")
  ) {
    return "paired";
  }
  return "";
}

function normalizeDeviceLists(value) {
  const pending = [];
  const paired = [];
  const pendingKeys = new Set();
  const pairedKeys = new Set();

  function toKey(entry, prefix) {
    const id =
      entry.requestId ??
      entry.deviceId ??
      entry.id ??
      entry.request_id ??
      entry.device_id;
    return `${prefix}:${id == null ? JSON.stringify(entry) : String(id)}`;
  }

  function pushUnique(target, keySet, entry, prefix) {
    const key = toKey(entry, prefix);
    if (keySet.has(key)) return;
    keySet.add(key);
    target.push(entry);
  }

  function classifyAndPush(rawEntry, context = "") {
    const entry = normalizeDeviceEntry(rawEntry);
    if (!entry) return;

    const status = String(entry.status ?? entry.state ?? "").toLowerCase();
    if (context === "pending" || PENDING_DEVICE_STATUSES.has(status)) {
      pushUnique(pending, pendingKeys, entry, "pending");
      return;
    }
    if (context === "paired" || APPROVED_DEVICE_STATUSES.has(status)) {
      pushUnique(paired, pairedKeys, entry, "paired");
      return;
    }

    if (entry.requestId && !entry.deviceId) {
      pushUnique(pending, pendingKeys, entry, "pending");
      return;
    }
    pushUnique(paired, pairedKeys, entry, "paired");
  }

  function walk(node, context = "") {
    if (!node) return;

    if (Array.isArray(node)) {
      for (const item of node) {
        if (looksLikeDeviceEntry(item)) {
          classifyAndPush(item, context);
        } else {
          walk(item, context);
        }
      }
      return;
    }

    if (typeof node !== "object") return;

    if (looksLikeDeviceEntry(node)) {
      classifyAndPush(node, context);
      return;
    }

    for (const [key, val] of Object.entries(node)) {
      const nextContext = classifyDeviceContext(key) || context;
      walk(val, nextContext);
    }
  }

  walk(value, "");

  const toTs = (entry) => {
    const t = Date.parse(String(entry.createdAt ?? ""));
    return Number.isNaN(t) ? 0 : t;
  };
  pending.sort((a, b) => toTs(b) - toTs(a));
  paired.sort((a, b) => toTs(b) - toTs(a));

  return { pending, paired };
}

const VALID_AUTH_CHOICES = [
  "provider-from-env",
  "openai-api-key",
  "apiKey",
  "gemini-api-key",
  "openrouter-api-key",
  "ai-gateway-api-key",
  "moonshot-api-key",
  "kimi-code-api-key",
  "zai-api-key",
  "minimax-api",
  "minimax-api-lightning",
  "qwen-portal",
  "github-copilot",
  "copilot-proxy",
  "synthetic-api-key",
  "opencode-zen",
  "volcengine-plan",
  "bedrock",
  "bailian",
  "ollama",
  "custom-provider",
];

const CUSTOM_PROVIDER_CHOICES = [
  "volcengine-plan",
  "bedrock",
  "bailian",
  "ollama",
  "custom-provider",
];

function isCustomProvider(authChoice) {
  return CUSTOM_PROVIDER_CHOICES.includes(authChoice);
}

function listAuthProfileStorePaths() {
  const files = [];
  const legacy = path.join(STATE_DIR, "agent", "auth-profiles.json");
  if (fs.existsSync(legacy)) files.push(legacy);

  const agentsRoot = path.join(STATE_DIR, "agents");
  if (fs.existsSync(agentsRoot)) {
    for (const entry of fs.readdirSync(agentsRoot, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const p = path.join(agentsRoot, entry.name, "agent", "auth-profiles.json");
      if (fs.existsSync(p)) files.push(p);
    }
  }

  if (files.length === 0) {
    files.push(path.join(STATE_DIR, "agents", "main", "agent", "auth-profiles.json"));
  }

  return [...new Set(files)];
}

function sanitizeBootstrapOpenAIEnvFile() {
  const envPath = path.join(STATE_DIR, ".env");
  if (!fs.existsSync(envPath)) return { changed: false, removed: 0 };

  const original = fs.readFileSync(envPath, "utf8");
  const lines = original.split(/\r?\n/);
  let removed = 0;
  const kept = [];

  for (const line of lines) {
    if (
      /^\s*OPENAI_API_KEY\s*=/.test(line) &&
      line.includes(CUSTOM_PROVIDER_BOOTSTRAP_OPENAI_KEY)
    ) {
      removed += 1;
      continue;
    }
    kept.push(line);
  }

  if (!removed) return { changed: false, removed: 0 };

  fs.writeFileSync(envPath, `${kept.join("\n").replace(/\n+$/, "")}\n`, "utf8");
  return { changed: true, removed };
}

function syncAuthProfileStores({ providerName, profileId, apiKey }) {
  const storePaths = listAuthProfileStorePaths();
  let changedFiles = 0;
  let parseErrors = 0;
  let removedProfiles = 0;

  for (const storePath of storePaths) {
    try {
      let store = {};
      if (fs.existsSync(storePath)) {
        const raw = fs.readFileSync(storePath, "utf8");
        store = raw.trim() ? JSON.parse(raw) : {};
      }

      if (!store || typeof store !== "object" || Array.isArray(store)) store = {};
      if (
        !store.profiles ||
        typeof store.profiles !== "object" ||
        Array.isArray(store.profiles)
      ) {
        store.profiles = {};
      }

      let changed = false;
      for (const [id, profile] of Object.entries(store.profiles)) {
        const p = profile || {};
        const pKey = typeof p.key === "string" ? p.key : "";
        const isPlaceholder =
          pKey.includes(CUSTOM_PROVIDER_BOOTSTRAP_OPENAI_KEY) ||
          pKey.includes("placeholder-for-custom-provider");
        if (id === "openai:default" || isPlaceholder) {
          delete store.profiles[id];
          removedProfiles += 1;
          changed = true;
        }
      }

      if (apiKey) {
        store.profiles[profileId] = {
          type: "api_key",
          provider: providerName,
          key: apiKey,
        };
        changed = true;
      }

      if (
        store.usageStats &&
        typeof store.usageStats === "object" &&
        !Array.isArray(store.usageStats)
      ) {
        for (const usageId of Object.keys(store.usageStats)) {
          if (!store.profiles[usageId]) {
            delete store.usageStats[usageId];
            changed = true;
          }
        }
      }

      if (!changed && fs.existsSync(storePath)) continue;

      fs.mkdirSync(path.dirname(storePath), { recursive: true });
      fs.writeFileSync(storePath, `${JSON.stringify(store, null, 2)}\n`, {
        encoding: "utf8",
        mode: 0o600,
      });
      changedFiles += 1;

      const authCachePath = path.join(path.dirname(storePath), "auth.json");
      if (fs.existsSync(authCachePath)) {
        try {
          fs.rmSync(authCachePath, { force: true });
        } catch {}
      }
    } catch {
      parseErrors += 1;
    }
  }

  return {
    ok: parseErrors === 0,
    changedFiles,
    parseErrors,
    removedProfiles,
    inspectedFiles: storePaths.length,
  };
}

function sanitizeDefaultModelEntries({ providerName, modelId }) {
  const cfgPath = configPath();
  if (!fs.existsSync(cfgPath)) {
    return { ok: true, changed: false, removed: 0, reason: "config-not-found" };
  }

  const fullModel =
    providerName && modelId ? `${providerName}/${modelId}` : null;

  try {
    const raw = fs.readFileSync(cfgPath, "utf8");
    const cfg = raw.trim() ? JSON.parse(raw) : {};
    if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) {
      return { ok: false, changed: false, removed: 0, reason: "invalid-config-json" };
    }

    if (!cfg.agents || typeof cfg.agents !== "object" || Array.isArray(cfg.agents)) {
      cfg.agents = {};
    }
    if (
      !cfg.agents.defaults ||
      typeof cfg.agents.defaults !== "object" ||
      Array.isArray(cfg.agents.defaults)
    ) {
      cfg.agents.defaults = {};
    }

    let changed = false;
    let removed = 0;

    if (fullModel) {
      if (
        !cfg.agents.defaults.model ||
        typeof cfg.agents.defaults.model !== "object" ||
        Array.isArray(cfg.agents.defaults.model)
      ) {
        cfg.agents.defaults.model = {};
      }
      if (cfg.agents.defaults.model.primary !== fullModel) {
        cfg.agents.defaults.model.primary = fullModel;
        changed = true;
      }
    }

    const currentModels =
      cfg.agents.defaults.models &&
      typeof cfg.agents.defaults.models === "object" &&
      !Array.isArray(cfg.agents.defaults.models)
        ? cfg.agents.defaults.models
        : {};

    const nextModels = {};
    if (fullModel) {
      const existingMain = currentModels[fullModel];
      nextModels[fullModel] =
        existingMain && typeof existingMain === "object" && !Array.isArray(existingMain)
          ? existingMain
          : {};
    }

    for (const [key, value] of Object.entries(currentModels)) {
      if (key === fullModel) continue;
      if (key.startsWith("openai/")) {
        removed += 1;
        changed = true;
        continue;
      }
      nextModels[key] = value;
    }

    if (JSON.stringify(nextModels) !== JSON.stringify(currentModels)) {
      cfg.agents.defaults.models = nextModels;
      changed = true;
    }

    if (!changed) {
      return { ok: true, changed: false, removed, reason: "no-change" };
    }

    fs.writeFileSync(cfgPath, `${JSON.stringify(cfg, null, 2)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });
    return { ok: true, changed: true, removed, reason: "updated" };
  } catch {
    return { ok: false, changed: false, removed: 0, reason: "parse-or-write-error" };
  }
}

function inferProviderName(entry = {}) {
  const model = String(entry.model || "").trim();
  if (isCustomProvider(entry.authChoice)) {
    if (entry.authChoice === "custom-provider") {
      return (entry.customProviderName || "custom").trim() || "custom";
    }
    return String(entry.authChoice || "").trim() || "custom";
  }
  if (entry.selectedGroup && entry.selectedGroup !== "provider-from-env") {
    return String(entry.selectedGroup).trim();
  }
  if (model.includes("/")) {
    return model.split("/")[0].trim();
  }
  return String(entry.authChoice || "openai").trim();
}

async function configureStandardProvider(entry) {
  const providerName = inferProviderName(entry);
  const apiKey = String(entry.authSecret || "").trim();
  const profileId = `${providerName}:default`;
  let extra = "";
  let ok = true;

  if (!apiKey) {
    return { ok: true, providerName, output: `[provider] ${providerName} has no api key, skipped auth profile sync\n` };
  }

  const authProfile = {
    provider: providerName,
    mode: "api_key",
  };
  const authResult = await runCmd(
    OPENCLAW_NODE,
    clawArgs([
      "config",
      "set",
      "--json",
      `auth.profiles.${profileId}`,
      JSON.stringify(authProfile),
    ]),
  );
  extra += `[provider] auth.profiles.${profileId} exit=${authResult.code}\n`;
  if (authResult.output) extra += authResult.output;
  if (authResult.code !== 0) ok = false;

  const authOrderResult = await runCmd(
    OPENCLAW_NODE,
    clawArgs([
      "config",
      "set",
      "--json",
      `auth.order.${providerName}`,
      JSON.stringify([profileId]),
    ]),
  );
  extra += `[provider] auth.order.${providerName} exit=${authOrderResult.code}\n`;
  if (authOrderResult.output) extra += authOrderResult.output;
  if (authOrderResult.code !== 0) ok = false;

  const syncAuthStoresResult = syncAuthProfileStores({
    providerName,
    profileId,
    apiKey,
  });
  extra +=
    `[provider] sync auth-profiles changed=${syncAuthStoresResult.changedFiles} ` +
    `removed=${syncAuthStoresResult.removedProfiles} ` +
    `files=${syncAuthStoresResult.inspectedFiles} ` +
    `parseErrors=${syncAuthStoresResult.parseErrors}\n`;
  if (!syncAuthStoresResult.ok) ok = false;

  return { ok, providerName, output: extra };
}

function normalizeAgentsFromPayload(payloadAgents, providerEntries) {
  const providerById = new Map();
  for (const p of providerEntries) {
    providerById.set(String(p.id || ""), String(p.model || "").trim());
  }

  const rawAgents = Array.isArray(payloadAgents) ? payloadAgents : [];
  const normalized = [];
  for (let i = 0; i < rawAgents.length; i++) {
    const raw = rawAgents[i] || {};
    let name = String(raw.name || "").trim() || `agent${i + 1}`;
    name = name.replace(/[^A-Za-z0-9_-]/g, "-");
    if (!name) name = `agent${i + 1}`;

    const providerId = String(raw.providerId || "").trim();
    const fromProvider = providerId ? providerById.get(providerId) : "";
    const model = String(raw.model || "").trim() || String(fromProvider || "").trim();
    if (!model) continue;

    normalized.push({ name, model });
  }

  if (normalized.length === 0) {
    const fallback = String(providerEntries[0]?.model || "").trim();
    if (fallback) {
      normalized.push({ name: "primary", model: fallback });
    }
  }

  const usedNames = new Set();
  for (let i = 0; i < normalized.length; i++) {
    const base = normalized[i].name;
    let candidate = base;
    let suffix = 2;
    while (usedNames.has(candidate)) {
      candidate = `${base}-${suffix}`;
      suffix += 1;
    }
    normalized[i].name = candidate;
    usedNames.add(candidate);
  }

  if (!normalized.some((a) => a.name === "primary") && normalized.length > 0) {
    normalized[0].name = "primary";
  }

  return normalized;
}

function applyAgentsModelConfig(agents) {
  const cfgPath = configPath();
  if (!fs.existsSync(cfgPath)) {
    return {
      ok: false,
      changed: false,
      primaryModel: "",
      reason: "config-not-found",
    };
  }

  try {
    const raw = fs.readFileSync(cfgPath, "utf8");
    const cfg = raw.trim() ? JSON.parse(raw) : {};
    if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) {
      return {
        ok: false,
        changed: false,
        primaryModel: "",
        reason: "invalid-config-json",
      };
    }

    if (!cfg.agents || typeof cfg.agents !== "object" || Array.isArray(cfg.agents)) {
      cfg.agents = {};
    }
    if (
      !cfg.agents.defaults ||
      typeof cfg.agents.defaults !== "object" ||
      Array.isArray(cfg.agents.defaults)
    ) {
      cfg.agents.defaults = {};
    }

    const previousModelMap =
      cfg.agents.defaults.model &&
      typeof cfg.agents.defaults.model === "object" &&
      !Array.isArray(cfg.agents.defaults.model)
        ? cfg.agents.defaults.model
        : {};
    const previousModels =
      cfg.agents.defaults.models &&
      typeof cfg.agents.defaults.models === "object" &&
      !Array.isArray(cfg.agents.defaults.models)
        ? cfg.agents.defaults.models
        : {};

    const nextModelMap = {};
    for (const a of agents) {
      nextModelMap[a.name] = a.model;
    }

    let primaryModel = String(nextModelMap.primary || "").trim();
    if (!primaryModel && agents[0]) {
      primaryModel = String(agents[0].model || "").trim();
      if (primaryModel) {
        nextModelMap.primary = primaryModel;
      }
    }

    const nextModels = {};
    for (const model of new Set(Object.values(nextModelMap).filter(Boolean))) {
      const existing = previousModels[model];
      nextModels[model] =
        existing && typeof existing === "object" && !Array.isArray(existing)
          ? existing
          : {};
    }

    const changed =
      JSON.stringify(previousModelMap) !== JSON.stringify(nextModelMap) ||
      JSON.stringify(previousModels) !== JSON.stringify(nextModels);

    if (!changed) {
      return { ok: true, changed: false, primaryModel, reason: "no-change" };
    }

    cfg.agents.defaults.model = nextModelMap;
    cfg.agents.defaults.models = nextModels;

    fs.writeFileSync(cfgPath, `${JSON.stringify(cfg, null, 2)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });

    return { ok: true, changed: true, primaryModel, reason: "updated" };
  } catch {
    return {
      ok: false,
      changed: false,
      primaryModel: "",
      reason: "parse-or-write-error",
    };
  }
}

async function configureCustomProvider(payload) {
  const providerName =
    payload.authChoice === "custom-provider"
      ? (payload.customProviderName || "custom").trim()
      : payload.authChoice;
  const baseUrl = (payload.customBaseUrl || "").trim();
  const rawApiType = (payload.customApiType || "openai-completions").trim();
  const apiType =
    rawApiType === "anthropic" ? "anthropic-messages" : rawApiType;
  const modelId = (payload.customModelId || "").trim();
  const apiKey = (payload.authSecret || "").trim();
  const profileId = `${providerName}:default`;

  let extra = "";
  let ok = true;

  // Write provider config to models.providers
  const providerCfg = { api: apiType };
  if (baseUrl) providerCfg.baseUrl = baseUrl;
  if (apiKey) providerCfg.apiKey = apiKey;
  if (modelId) {
    providerCfg.models = [{ id: modelId, name: modelId }];
  }

  const providerResult = await runCmd(
    OPENCLAW_NODE,
    clawArgs([
      "config",
      "set",
      "--json",
      `models.providers.${providerName}`,
      JSON.stringify(providerCfg),
    ]),
  );
  extra += `[custom-provider] models.providers.${providerName} exit=${providerResult.code}\n`;
  if (providerResult.output) extra += providerResult.output;
  if (providerResult.code !== 0) ok = false;

  // Write auth profile metadata (credentials are read from models.providers.*.apiKey)
  if (apiKey) {
    const authProfile = {
      provider: providerName,
      mode: "api_key",
    };
    const authResult = await runCmd(
      OPENCLAW_NODE,
      clawArgs([
        "config",
        "set",
        "--json",
        `auth.profiles.${profileId}`,
        JSON.stringify(authProfile),
      ]),
    );
    extra += `[custom-provider] auth.profiles.${profileId} exit=${authResult.code}\n`;
    if (authResult.output) extra += authResult.output;
    if (authResult.code !== 0) ok = false;

    const authOrderResult = await runCmd(
      OPENCLAW_NODE,
      clawArgs([
        "config",
        "set",
        "--json",
        `auth.order.${providerName}`,
        JSON.stringify([profileId]),
      ]),
    );
    extra += `[custom-provider] auth.order.${providerName} exit=${authOrderResult.code}\n`;
    if (authOrderResult.output) extra += authOrderResult.output;
    if (authOrderResult.code !== 0) ok = false;

  }

  const syncAuthStoresResult = syncAuthProfileStores({
    providerName,
    profileId,
    apiKey,
  });
  extra +=
    `[custom-provider] sync auth-profiles changed=${syncAuthStoresResult.changedFiles} ` +
    `removed=${syncAuthStoresResult.removedProfiles} ` +
    `files=${syncAuthStoresResult.inspectedFiles} ` +
    `parseErrors=${syncAuthStoresResult.parseErrors}\n`;
  if (!syncAuthStoresResult.ok) ok = false;

  const cleanBootstrapEnvResult = sanitizeBootstrapOpenAIEnvFile();
  extra +=
    `[custom-provider] sanitize ${path.join(STATE_DIR, ".env")} ` +
    `changed=${cleanBootstrapEnvResult.changed} removed=${cleanBootstrapEnvResult.removed}\n`;

  const unsetOpenAIProfile = await runCmd(
    OPENCLAW_NODE,
    clawArgs(["config", "unset", "auth.profiles.openai:default"]),
  );
  extra += `[custom-provider] unset auth.profiles.openai:default exit=${unsetOpenAIProfile.code}\n`;
  if (unsetOpenAIProfile.output) extra += unsetOpenAIProfile.output;

  const unsetOpenAIOrder = await runCmd(
    OPENCLAW_NODE,
    clawArgs(["config", "unset", "auth.order.openai"]),
  );
  extra += `[custom-provider] unset auth.order.openai exit=${unsetOpenAIOrder.code}\n`;
  if (unsetOpenAIOrder.output) extra += unsetOpenAIOrder.output;

  // Set model
  if (modelId) {
    const fullModel = `${providerName}/${modelId}`;
    const modelResult = await runCmd(
      OPENCLAW_NODE,
      clawArgs(["models", "set", fullModel]),
    );
    extra += `[custom-provider] models set ${fullModel} exit=${modelResult.code}\n`;
    if (modelResult.output) extra += modelResult.output;
    if (modelResult.code !== 0) ok = false;
  }

  const sanitizeModelsResult = sanitizeDefaultModelEntries({
    providerName,
    modelId,
  });
  extra +=
    `[custom-provider] sanitize agents.defaults.models changed=${sanitizeModelsResult.changed} ` +
    `removed=${sanitizeModelsResult.removed} reason=${sanitizeModelsResult.reason}\n`;
  if (!sanitizeModelsResult.ok) ok = false;

  return { ok, output: extra };
}

function validatePayload(payload) {
  if (payload.authChoice && !VALID_AUTH_CHOICES.includes(payload.authChoice)) {
    return `Invalid authChoice: ${payload.authChoice}`;
  }
  const stringFields = [
    "telegramToken",
    "discordToken",
    "slackBotToken",
    "slackAppToken",
    "authSecret",
    "model",
    "customProviderName",
    "customBaseUrl",
    "customApiType",
    "customModelId",
  ];
  for (const field of stringFields) {
    if (payload[field] !== undefined && typeof payload[field] !== "string") {
      return `Invalid ${field}: must be a string`;
    }
  }
  if (
    payload.authChoice === "custom-provider" &&
    payload.customProviderName &&
    !/^[A-Za-z0-9_-]+$/.test(payload.customProviderName)
  ) {
    return "Invalid customProviderName: only letters, numbers, underscore, and hyphen are allowed";
  }
  if (payload.customApiType) {
    const validApiTypes = [
      "openai-completions",
      "openai-responses",
      "anthropic",
      "anthropic-messages",
      "google-generative-ai",
    ];
    if (!validApiTypes.includes(payload.customApiType)) {
      return `Invalid customApiType: must be one of ${validApiTypes.join(", ")}`;
    }
  }

  if (payload.providers !== undefined) {
    if (!Array.isArray(payload.providers) || payload.providers.length === 0) {
      return "Invalid providers: must be a non-empty array";
    }
    for (const provider of payload.providers) {
      if (!provider || typeof provider !== "object" || Array.isArray(provider)) {
        return "Invalid providers: each provider must be an object";
      }
      const providerError = validatePayload(provider);
      if (providerError) return `Invalid provider entry: ${providerError}`;
    }
  }

  if (payload.agents !== undefined) {
    if (!Array.isArray(payload.agents) || payload.agents.length === 0) {
      return "Invalid agents: must be a non-empty array";
    }
    for (const agent of payload.agents) {
      if (!agent || typeof agent !== "object" || Array.isArray(agent)) {
        return "Invalid agents: each agent must be an object";
      }
      for (const k of ["name", "providerId", "model"]) {
        if (agent[k] !== undefined && typeof agent[k] !== "string") {
          return `Invalid agents.${k}: must be a string`;
        }
      }
    }
  }

  return null;
}

app.post("/setup/api/run", requireSetupAuth, async (req, res) => {
  try {
    if (isConfigured()) {
      await ensureGatewayRunning();
      return res.json({
        ok: true,
        output:
          "Already configured.\nUse Reset setup if you want to rerun onboarding.\n",
      });
    }

    fs.mkdirSync(STATE_DIR, { recursive: true });
    fs.mkdirSync(WORKSPACE_DIR, { recursive: true });

    const rawPayload = req.body || {};
    let payload = rawPayload;

    if (Array.isArray(rawPayload.providers) && rawPayload.providers.length > 0) {
      const transformedProviders = [];
      for (const provider of rawPayload.providers) {
        const p = provider || {};
        if (p.authChoice === "provider-from-env") {
          const transformed = buildPayloadFromEnvProvider(p);
          if (!transformed.ok) {
            return res.status(400).json({ ok: false, output: transformed.error });
          }
          transformedProviders.push({ ...p, ...transformed.payload });
        } else {
          transformedProviders.push(p);
        }
      }
      const firstProvider = transformedProviders[0] || {};
      payload = {
        ...rawPayload,
        providers: transformedProviders,
        authChoice: firstProvider.authChoice || rawPayload.authChoice,
        authSecret: firstProvider.authSecret || rawPayload.authSecret,
        model: firstProvider.model || rawPayload.model,
        customProviderName:
          firstProvider.customProviderName || rawPayload.customProviderName,
        customBaseUrl: firstProvider.customBaseUrl || rawPayload.customBaseUrl,
        customApiType: firstProvider.customApiType || rawPayload.customApiType,
        customModelId: firstProvider.customModelId || rawPayload.customModelId,
      };
    } else if (rawPayload.authChoice === "provider-from-env") {
      const transformed = buildPayloadFromEnvProvider(rawPayload);
      if (!transformed.ok) {
        return res.status(400).json({ ok: false, output: transformed.error });
      }
      payload = transformed.payload;
    }

    const validationError = validatePayload(payload);
    if (validationError) {
      return res.status(400).json({ ok: false, output: validationError });
    }
    const onboardArgs = buildOnboardArgs(payload);
    const onboard = await runCmd(OPENCLAW_NODE, clawArgs(onboardArgs));

    let extra = "";
    extra += `\n[setup] Onboarding exit=${onboard.code} configured=${isConfigured()}\n`;

    const ok = onboard.code === 0 && isConfigured();

    if (ok) {
      extra += "\n[setup] Configuring gateway settings...\n";

      let allowInsecureResult = await runCmd(
        OPENCLAW_NODE,
        clawArgs([
          "config",
          "set",
          "--json",
          "gateway.controlUi.allowInsecureAuth",
          "true",
        ]),
      );
      extra += `[config] gateway.controlUi.allowInsecureAuth=true exit=${allowInsecureResult.code}\n`;
      if (allowInsecureResult.code !== 0) {
        const allowInsecureFallbackResult = await runCmd(
          OPENCLAW_NODE,
          clawArgs([
            "config",
            "set",
            "gateway.controlUi.allowInsecureAuth",
            "true",
          ]),
        );
        extra += `[config] gateway.controlUi.allowInsecureAuth=true (fallback) exit=${allowInsecureFallbackResult.code}\n`;
        if (allowInsecureFallbackResult.code === 0) {
          allowInsecureResult = allowInsecureFallbackResult;
        }
      }

      const allowInsecureVerify = await runCmd(
        OPENCLAW_NODE,
        clawArgs(["config", "get", "gateway.controlUi.allowInsecureAuth"]),
      );
      const allowInsecureCurrent = allowInsecureVerify.output.trim();
      extra += `[config] gateway.controlUi.allowInsecureAuth current=${allowInsecureCurrent || "(empty)"}\n`;
      if (!/\btrue\b/i.test(allowInsecureCurrent)) {
        extra += "[warn] gateway.controlUi.allowInsecureAuth was not confirmed as true; Control UI may still require device pairing.\n";
      }

      const tokenResult = await runCmd(
        OPENCLAW_NODE,
        clawArgs([
          "config",
          "set",
          "gateway.auth.token",
          OPENCLAW_GATEWAY_TOKEN,
        ]),
      );
      extra += `[config] gateway.auth.token exit=${tokenResult.code}\n`;

      const proxiesResult = await runCmd(
        OPENCLAW_NODE,
        clawArgs([
          "config",
          "set",
          "--json",
          "gateway.trustedProxies",
          '["127.0.0.1"]',
        ]),
      );
      extra += `[config] gateway.trustedProxies exit=${proxiesResult.code}\n`;

      const providerEntries =
        Array.isArray(payload.providers) && payload.providers.length > 0
          ? payload.providers.map((p, idx) => ({
              ...p,
              id: String(p.id || `provider-${idx + 1}`),
              model: String(p.model || "").trim(),
            }))
          : [{ ...payload, id: "provider-1", model: String(payload.model || "").trim() }];
      extra += `\n[setup] Configuring ${providerEntries.length} provider(s)...\n`;

      for (let i = 0; i < providerEntries.length; i++) {
        const provider = providerEntries[i];
        extra += `[setup] Provider #${i + 1} authChoice=${provider.authChoice || "(empty)"}\n`;
        if (isCustomProvider(provider.authChoice)) {
          const customProviderResult = await configureCustomProvider(provider);
          extra += customProviderResult.output;
          if (!customProviderResult.ok) {
            return res.status(500).json({
              ok: false,
              output:
                `${onboard.output}${extra}\n` +
                `[setup] Provider #${i + 1} custom provider setup failed.\n`,
            });
          }
        } else {
          const stdProviderResult = await configureStandardProvider(provider);
          extra += stdProviderResult.output;
          if (!stdProviderResult.ok) {
            return res.status(500).json({
              ok: false,
              output:
                `${onboard.output}${extra}\n` +
                `[setup] Provider #${i + 1} standard provider setup failed.\n`,
            });
          }
        }
      }

      const normalizedAgents = normalizeAgentsFromPayload(payload.agents, providerEntries);
      const applyAgentsResult = applyAgentsModelConfig(normalizedAgents);
      extra +=
        `[setup] apply agents.defaults.model changed=${applyAgentsResult.changed} ` +
        `reason=${applyAgentsResult.reason} primary=${applyAgentsResult.primaryModel || "(empty)"}\n`;
      if (!applyAgentsResult.ok) {
        return res.status(500).json({
          ok: false,
          output:
            `${onboard.output}${extra}\n` +
            "[setup] Failed to write multi-agent model config.\n",
        });
      }

      if (applyAgentsResult.primaryModel) {
        const modelResult = await runCmd(
          OPENCLAW_NODE,
          clawArgs(["models", "set", applyAgentsResult.primaryModel]),
        );
        extra += `[models set] exit=${modelResult.code}\n${modelResult.output || ""}`;
        if (modelResult.code !== 0) {
          return res.status(500).json({
            ok: false,
            output:
              `${onboard.output}${extra}\n` +
              "[setup] Primary model setup failed. Check logs above.\n",
          });
        }
      }

      async function configureChannel(name, cfgObj) {
        const set = await runCmd(
          OPENCLAW_NODE,
          clawArgs([
            "config",
            "set",
            "--json",
            `channels.${name}`,
            JSON.stringify(cfgObj),
          ]),
        );
        const get = await runCmd(
          OPENCLAW_NODE,
          clawArgs(["config", "get", `channels.${name}`]),
        );
        return (
          `\n[${name} config] exit=${set.code} (output ${set.output.length} chars)\n${set.output || "(no output)"}` +
          `\n[${name} verify] exit=${get.code} (output ${get.output.length} chars)\n${get.output || "(no output)"}`
        );
      }

      if (payload.telegramToken?.trim()) {
        extra += await configureChannel("telegram", {
          enabled: true,
          dmPolicy: "pairing",
          botToken: payload.telegramToken.trim(),
          groupPolicy: "allowlist",
          streamMode: "partial",
        });
      }

      if (payload.discordToken?.trim()) {
        extra += await configureChannel("discord", {
          enabled: true,
          token: payload.discordToken.trim(),
          groupPolicy: "allowlist",
          dm: { policy: "pairing" },
        });
      }

      if (payload.slackBotToken?.trim() || payload.slackAppToken?.trim()) {
        extra += await configureChannel("slack", {
          enabled: true,
          botToken: payload.slackBotToken?.trim() || undefined,
          appToken: payload.slackAppToken?.trim() || undefined,
        });
      }

      extra += "\n[setup] Starting gateway...\n";
      await restartGateway();
      extra += "[setup] Gateway started.\n";
    }

    return res.status(ok ? 200 : 500).json({
      ok,
      output: `${onboard.output}${extra}`,
    });
  } catch (err) {
    console.error("[/setup/api/run] error:", err);
    return res
      .status(500)
      .json({ ok: false, output: `Internal error: ${String(err)}` });
  }
});

app.get("/setup/api/debug", requireSetupAuth, async (_req, res) => {
  const v = await runCmd(OPENCLAW_NODE, clawArgs(["--version"]));
  const help = await runCmd(
    OPENCLAW_NODE,
    clawArgs(["channels", "add", "--help"]),
  );
  res.json({
    wrapper: {
      node: process.version,
      port: PORT,
      stateDir: STATE_DIR,
      workspaceDir: WORKSPACE_DIR,
      configPath: configPath(),
      gatewayTokenFromEnv: Boolean(process.env.OPENCLAW_GATEWAY_TOKEN?.trim()),
      gatewayTokenPersisted: fs.existsSync(
        path.join(STATE_DIR, "gateway.token"),
      ),
      railwayCommit: process.env.RAILWAY_GIT_COMMIT_SHA || null,
    },
    openclaw: {
      entry: OPENCLAW_ENTRY,
      node: OPENCLAW_NODE,
      version: v.output.trim(),
      channelsAddHelpIncludesTelegram: help.output.includes("telegram"),
    },
  });
});

app.post("/setup/api/pairing/approve", requireSetupAuth, async (req, res) => {
  const { channel, code } = req.body || {};
  if (!channel || !code) {
    return res
      .status(400)
      .json({ ok: false, error: "Missing channel or code" });
  }
  const r = await runCmd(
    OPENCLAW_NODE,
    clawArgs(["pairing", "approve", String(channel), String(code)]),
  );
  return res
    .status(r.code === 0 ? 200 : 500)
    .json({ ok: r.code === 0, output: r.output });
});

app.post("/setup/api/reset", requireSetupAuth, async (_req, res) => {
  try {
    fs.rmSync(configPath(), { force: true });
    res
      .type("text/plain")
      .send("OK - deleted config file. You can rerun setup now.");
  } catch (err) {
    res.status(500).type("text/plain").send(String(err));
  }
});

app.post("/setup/api/doctor", requireSetupAuth, async (_req, res) => {
  const args = ["doctor", "--non-interactive", "--repair"];
  const result = await runCmd(OPENCLAW_NODE, clawArgs(args));
  return res.status(result.code === 0 ? 200 : 500).json({
    ok: result.code === 0,
    output: result.output,
  });
});

app.get("/setup/api/devices", requireSetupAuth, async (_req, res) => {
  const args = ["devices", "list", "--json", "--token", OPENCLAW_GATEWAY_TOKEN];
  const result = await runCmd(OPENCLAW_NODE, clawArgs(args));
  const data = parseJsonFromOutput(result.output);
  const normalized = normalizeDeviceLists(data);
  res.set("Cache-Control", "no-store");
  return res.json({
    ok: result.code === 0,
    data,
    pending: normalized.pending,
    paired: normalized.paired,
    raw: result.output,
  });
});

app.post("/setup/api/devices/approve", requireSetupAuth, async (req, res) => {
  const { requestId } = req.body || {};
  const args = ["devices", "approve"];
  if (requestId) {
    args.push(String(requestId));
  } else {
    args.push("--latest");
  }
  args.push("--token", OPENCLAW_GATEWAY_TOKEN);
  const result = await runCmd(OPENCLAW_NODE, clawArgs(args));
  return res
    .status(result.code === 0 ? 200 : 500)
    .json({ ok: result.code === 0, output: result.output });
});

app.post("/setup/api/devices/reject", requireSetupAuth, async (req, res) => {
  const { requestId } = req.body || {};
  if (!requestId) {
    return res.status(400).json({ ok: false, error: "Missing requestId" });
  }
  const args = [
    "devices", "reject", String(requestId),
    "--token", OPENCLAW_GATEWAY_TOKEN,
  ];
  const result = await runCmd(OPENCLAW_NODE, clawArgs(args));
  return res
    .status(result.code === 0 ? 200 : 500)
    .json({ ok: result.code === 0, output: result.output });
});

app.get("/setup/api/export", requireSetupAuth, async (_req, res) => {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const zipName = `openclaw-export-${timestamp}.zip`;
  const tmpZip = path.join(os.tmpdir(), zipName);

  try {
    const dirsToExport = [];
    if (fs.existsSync(STATE_DIR)) dirsToExport.push(STATE_DIR);
    if (fs.existsSync(WORKSPACE_DIR)) dirsToExport.push(WORKSPACE_DIR);

    if (dirsToExport.length === 0) {
      return res.status(404).json({ ok: false, error: "No data directories found to export." });
    }

    const zipArgs = ["-r", "-P", SETUP_PASSWORD, tmpZip, ...dirsToExport];
    const result = await runCmd("zip", zipArgs);

    if (result.code !== 0 || !fs.existsSync(tmpZip)) {
      return res.status(500).json({ ok: false, error: "Failed to create export archive.", output: result.output });
    }

    const stat = fs.statSync(tmpZip);
    res.set({
      "Content-Type": "application/zip",
      "Content-Disposition": `attachment; filename="${zipName}"`,
      "Content-Length": String(stat.size),
    });

    const stream = fs.createReadStream(tmpZip);
    stream.pipe(res);
    stream.on("end", () => {
      try { fs.rmSync(tmpZip, { force: true }); } catch {}
    });
    stream.on("error", (err) => {
      console.error("[export] stream error:", err);
      try { fs.rmSync(tmpZip, { force: true }); } catch {}
      if (!res.headersSent) {
        res.status(500).json({ ok: false, error: "Stream error during export." });
      }
    });
  } catch (err) {
    try { fs.rmSync(tmpZip, { force: true }); } catch {}
    console.error("[export] error:", err);
    return res.status(500).json({ ok: false, error: `Export failed: ${err.message}` });
  }
});

app.get("/tui", requireSetupAuth, (_req, res) => {
  if (!ENABLE_WEB_TUI) {
    return res
      .status(403)
      .type("text/plain")
      .send("Web TUI is disabled. Set ENABLE_WEB_TUI=true to enable it.");
  }
  if (!isConfigured()) {
    return res.redirect("/setup");
  }
  res.sendFile(path.join(process.cwd(), "src", "public", "tui.html"));
});

let activeTuiSession = null;

function verifyTuiAuth(req) {
  if (!SETUP_PASSWORD) return false;
  const header = req.headers.authorization || "";
  const [scheme, encoded] = header.split(" ");
  if (scheme !== "Basic" || !encoded) return false;
  const decoded = Buffer.from(encoded, "base64").toString("utf8");
  const idx = decoded.indexOf(":");
  const password = idx >= 0 ? decoded.slice(idx + 1) : "";
  const passwordHash = crypto.createHash("sha256").update(password).digest();
  const expectedHash = crypto.createHash("sha256").update(SETUP_PASSWORD).digest();
  return crypto.timingSafeEqual(passwordHash, expectedHash);
}

function createTuiWebSocketServer(httpServer) {
  const wss = new WebSocketServer({ noServer: true });

  wss.on("connection", (ws, req) => {
    const clientIp = req.socket?.remoteAddress || "unknown";
    console.log(`[tui] session started from ${clientIp}`);

    let ptyProcess = null;
    let idleTimer = null;
    let maxSessionTimer = null;

    activeTuiSession = {
      ws,
      pty: null,
      startedAt: Date.now(),
      lastActivity: Date.now(),
    };

    function resetIdleTimer() {
      if (activeTuiSession) {
        activeTuiSession.lastActivity = Date.now();
      }
      clearTimeout(idleTimer);
      idleTimer = setTimeout(() => {
        console.log("[tui] session idle timeout");
        ws.close(4002, "Idle timeout");
      }, TUI_IDLE_TIMEOUT_MS);
    }

    function spawnPty(cols, rows) {
      if (ptyProcess) return;

      console.log(`[tui] spawning PTY with ${cols}x${rows}`);
      ptyProcess = pty.spawn(OPENCLAW_NODE, clawArgs(["tui"]), {
        name: "xterm-256color",
        cols,
        rows,
        cwd: WORKSPACE_DIR,
        env: {
          ...process.env,
          OPENCLAW_STATE_DIR: STATE_DIR,
          OPENCLAW_WORKSPACE_DIR: WORKSPACE_DIR,
          TERM: "xterm-256color",
        },
      });

      if (activeTuiSession) {
        activeTuiSession.pty = ptyProcess;
      }

      idleTimer = setTimeout(() => {
        console.log("[tui] session idle timeout");
        ws.close(4002, "Idle timeout");
      }, TUI_IDLE_TIMEOUT_MS);

      maxSessionTimer = setTimeout(() => {
        console.log("[tui] max session duration reached");
        ws.close(4002, "Max session duration");
      }, TUI_MAX_SESSION_MS);

      ptyProcess.onData((data) => {
        if (ws.readyState === ws.OPEN) {
          ws.send(data);
        }
      });

      ptyProcess.onExit(({ exitCode, signal }) => {
        console.log(`[tui] PTY exited code=${exitCode} signal=${signal}`);
        if (ws.readyState === ws.OPEN) {
          ws.close(1000, "Process exited");
        }
      });
    }

    ws.on("message", (message) => {
      resetIdleTimer();
      try {
        const msg = JSON.parse(message.toString());
        if (msg.type === "resize" && msg.cols && msg.rows) {
          const cols = Math.min(Math.max(msg.cols, 10), 500);
          const rows = Math.min(Math.max(msg.rows, 5), 200);
          if (!ptyProcess) {
            spawnPty(cols, rows);
          } else {
            ptyProcess.resize(cols, rows);
          }
        } else if (msg.type === "input" && msg.data && ptyProcess) {
          ptyProcess.write(msg.data);
        }
      } catch (err) {
        console.warn(`[tui] invalid message: ${err.message}`);
      }
    });

    ws.on("close", () => {
      console.log("[tui] session closed");
      clearTimeout(idleTimer);
      clearTimeout(maxSessionTimer);
      if (ptyProcess) {
        try {
          ptyProcess.kill();
        } catch {}
      }
      activeTuiSession = null;
    });

    ws.on("error", (err) => {
      console.error(`[tui] WebSocket error: ${err.message}`);
    });
  });

  return wss;
}

const proxy = httpProxy.createProxyServer({
  target: GATEWAY_TARGET,
  ws: true,
  xfwd: true,
  changeOrigin: true,
  proxyTimeout: 120_000,
  timeout: 120_000,
});

proxy.on("error", (err, _req, res) => {
  console.error("[proxy]", err);
  if (res && typeof res.headersSent !== "undefined" && !res.headersSent) {
    res.writeHead(503, { "Content-Type": "text/html" });
    try {
      const html = fs.readFileSync(
        path.join(process.cwd(), "src", "public", "loading.html"),
        "utf8",
      );
      res.end(html);
    } catch {
      res.end("Gateway unavailable. Retrying...");
    }
  }
});

proxy.on("proxyReq", (proxyReq, req, res) => {
  proxyReq.setHeader("Authorization", `Bearer ${OPENCLAW_GATEWAY_TOKEN}`);
  proxyReq.setHeader("Origin", GATEWAY_TARGET);
});

proxy.on("proxyReqWs", (proxyReq, req, socket, options, head) => {
  proxyReq.setHeader("Authorization", `Bearer ${OPENCLAW_GATEWAY_TOKEN}`);
  proxyReq.setHeader("Origin", GATEWAY_TARGET);
});

app.use(async (req, res) => {
  if (!isConfigured() && !req.path.startsWith("/setup")) {
    return res.redirect("/setup");
  }

  if (isConfigured()) {
    if (!isGatewayReady()) {
      try {
        await ensureGatewayRunning();
      } catch {
        return res
          .status(503)
          .sendFile(path.join(process.cwd(), "src", "public", "loading.html"));
      }

      if (!isGatewayReady()) {
        return res
          .status(503)
          .sendFile(path.join(process.cwd(), "src", "public", "loading.html"));
      }
    }
  }

  if (req.path === "/openclaw" && !req.query.token) {
    return res.redirect(`/openclaw?token=${OPENCLAW_GATEWAY_TOKEN}`);
  }

  return proxy.web(req, res, { target: GATEWAY_TARGET });
});

const server = app.listen(PORT, () => {
  console.log(`[wrapper] listening on port ${PORT}`);
  console.log(`[wrapper] setup wizard: http://localhost:${PORT}/setup`);
  console.log(`[wrapper] web TUI: ${ENABLE_WEB_TUI ? "enabled" : "disabled"}`);
  console.log(`[wrapper] configured: ${isConfigured()}`);

  if (isConfigured()) {
    (async () => {
      try {
        console.log("[wrapper] running openclaw doctor --fix...");
        const dr = await runCmd(OPENCLAW_NODE, clawArgs(["doctor", "--fix"]));
        console.log(`[wrapper] doctor --fix exit=${dr.code}`);
        if (dr.output) console.log(dr.output);
      } catch (err) {
        console.warn(`[wrapper] doctor --fix failed: ${err.message}`);
      }
      await ensureGatewayRunning();
    })().catch((err) => {
      console.error(`[wrapper] failed to start gateway at boot: ${err.message}`);
    });
  }
});

const tuiWss = createTuiWebSocketServer(server);

server.on("upgrade", async (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === "/tui/ws") {
    if (!ENABLE_WEB_TUI) {
      socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
      socket.destroy();
      return;
    }

    if (!verifyTuiAuth(req)) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"OpenClaw TUI\"\r\n\r\n");
      socket.destroy();
      return;
    }

    if (activeTuiSession) {
      socket.write("HTTP/1.1 409 Conflict\r\n\r\n");
      socket.destroy();
      return;
    }

    tuiWss.handleUpgrade(req, socket, head, (ws) => {
      tuiWss.emit("connection", ws, req);
    });
    return;
  }

  if (!isConfigured()) {
    socket.destroy();
    return;
  }
  try {
    await ensureGatewayRunning();
  } catch (err) {
    console.warn(`[websocket] gateway not ready: ${err.message}`);
    socket.destroy();
    return;
  }
  proxy.ws(req, socket, head, { target: GATEWAY_TARGET });
});

async function gracefulShutdown(signal) {
  console.log(`[wrapper] received ${signal}, shutting down`);
  shuttingDown = true;

  if (setupRateLimiter.cleanupInterval) {
    clearInterval(setupRateLimiter.cleanupInterval);
  }

  if (activeTuiSession) {
    try {
      activeTuiSession.ws.close(1001, "Server shutting down");
      activeTuiSession.pty.kill();
    } catch {}
    activeTuiSession = null;
  }

  server.close();

  if (gatewayProc) {
    try {
      gatewayProc.kill("SIGTERM");
      await Promise.race([
        new Promise((resolve) => gatewayProc.on("exit", resolve)),
        new Promise((resolve) => setTimeout(resolve, 2000)),
      ]);
      if (gatewayProc && !gatewayProc.killed) {
        gatewayProc.kill("SIGKILL");
      }
    } catch (err) {
      console.warn(`[wrapper] error killing gateway: ${err.message}`);
    }
  }

  process.exit(0);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
