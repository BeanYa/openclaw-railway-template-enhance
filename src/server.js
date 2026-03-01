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

const DEFAULT_PROVIDER_BASE_URLS = Object.freeze({
  openai: "https://api.openai.com/v1",
  anthropic: "https://api.anthropic.com/v1",
  google: "https://generativelanguage.googleapis.com/v1beta",
  openrouter: "https://openrouter.ai/api/v1",
  "ai-gateway": "https://gateway.ai.vercel.com/v1",
  moonshot: "https://api.moonshot.cn/v1",
  zai: "https://open.bigmodel.cn/api/paas/v4",
  "zai-cn": "https://open.bigmodel.cn/api/paas/v4",
  "zai-global": "https://api.z.ai/api/paas/v4",
  "zai-coding-cn": "https://open.bigmodel.cn/api/coding/paas/v4",
  "zai-coding-global": "https://api.z.ai/api/coding/paas/v4",
  minimax: "https://api.minimax.chat/v1",
  "volcengine-plan": "https://ark.cn-beijing.volces.com/api/coding/v3",
  bedrock: "https://bedrock-runtime.us-east-1.amazonaws.com",
  bailian: "https://dashscope.aliyuncs.com/compatible-mode/v1",
  ollama: "http://localhost:11434/v1",
});

const ENV_PROVIDER_DEFINITIONS = Object.freeze({
  OPENAI: {
    providerName: "openai",
    selectedGroup: "openai",
    authChoice: "openai-api-key",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  ANTHROPIC: {
    providerName: "anthropic",
    selectedGroup: "anthropic",
    authChoice: "apiKey",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  GOOGLE: {
    providerName: "google",
    selectedGroup: "google",
    authChoice: "gemini-api-key",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  OPENROUTER: {
    providerName: "openrouter",
    selectedGroup: "openrouter",
    authChoice: "openrouter-api-key",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  AI_GATEWAY: {
    providerName: "ai-gateway",
    selectedGroup: "ai-gateway",
    authChoice: "ai-gateway-api-key",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  MOONSHOT: {
    providerName: "moonshot",
    selectedGroup: "moonshot",
    authChoice: "moonshot-api-key",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  ZAI: {
    providerName: "zai",
    selectedGroup: "zai",
    authChoice: "zai-api-key",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  ZAI_CN: {
    providerName: "zai-cn",
    selectedGroup: "zai-cn",
    authChoice: "zai-cn",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "openai-completions",
  },
  ZAI_GLOBAL: {
    providerName: "zai-global",
    selectedGroup: "zai-global",
    authChoice: "zai-global",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "openai-completions",
  },
  ZAI_CODING_CN: {
    providerName: "zai-coding-cn",
    selectedGroup: "zai-coding-cn",
    authChoice: "zai-coding-cn",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "openai-completions",
  },
  ZAI_CODING_GLOBAL: {
    providerName: "zai-coding-global",
    selectedGroup: "zai-coding-global",
    authChoice: "zai-coding-global",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "openai-completions",
  },
  MINIMAX: {
    providerName: "minimax",
    selectedGroup: "minimax",
    authChoice: "minimax-api",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  SYNTHETIC: {
    providerName: "synthetic",
    selectedGroup: "synthetic",
    authChoice: "synthetic-api-key",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  OPENCODE_ZEN: {
    providerName: "opencode-zen",
    selectedGroup: "opencode-zen",
    authChoice: "opencode-zen",
    customChoice: false,
    requiresCustomBaseUrl: false,
  },
  VOLCENGINE_PLAN: {
    providerName: "volcengine-plan",
    selectedGroup: "volcengine-plan",
    authChoice: "volcengine-plan",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "openai-completions",
  },
  BEDROCK: {
    providerName: "bedrock",
    selectedGroup: "bedrock",
    authChoice: "bedrock",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "anthropic-messages",
  },
  BAILIAN: {
    providerName: "bailian",
    selectedGroup: "bailian",
    authChoice: "bailian",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "openai-completions",
  },
  OLLAMA: {
    providerName: "ollama",
    selectedGroup: "ollama",
    authChoice: "ollama",
    customChoice: true,
    requiresCustomBaseUrl: false,
    customApiType: "openai-completions",
  },
  CUSTOM_PROVIDER: {
    providerName: "custom-provider",
    selectedGroup: "custom-provider",
    authChoice: "custom-provider",
    customChoice: true,
    requiresCustomBaseUrl: true,
    customApiType: "openai-completions",
  },
});

const VALID_ENV_AI_PROVIDERS = Object.freeze(
  Object.keys(ENV_PROVIDER_DEFINITIONS),
);

const AI_PROVIDER_RAW = process.env.AI_PROVIDER?.trim() || "";
const AI_PROVIDER = AI_PROVIDER_RAW.toUpperCase();
const PROVIDER_BASE_URL = process.env.PROVIDER_BASE_URL?.trim() || "";
const PROVIDER_API_KEY = process.env.PROVIDER_API_KEY?.trim() || "";

function resolveEnvProviderDefinition(value) {
  const key = String(value || "").trim().toUpperCase();
  return ENV_PROVIDER_DEFINITIONS[key] || null;
}

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

function normalizeEnvModel(rawModel, providerName) {
  const source = String(rawModel || "").trim();
  if (!source) return { model: "", modelId: "" };

  if (!source.includes("/")) {
    return {
      model: `${providerName}/${source}`,
      modelId: source,
    };
  }

  const parts = source.split("/");
  const modelId = parts.slice(1).join("/").trim();
  if (!modelId) return { model: "", modelId: "" };

  return {
    model: `${providerName}/${modelId}`,
    modelId,
  };
}

function validateEnvProviderConfig() {
  const enabled = Boolean(AI_PROVIDER_RAW);
  const providerDef = resolveEnvProviderDefinition(AI_PROVIDER);
  const requiresCustomBaseUrl = Boolean(providerDef?.requiresCustomBaseUrl);
  const builtinBaseUrl = providerDef
    ? DEFAULT_PROVIDER_BASE_URLS[providerDef.providerName] || ""
    : "";
  const effectiveProviderBaseUrl = requiresCustomBaseUrl
    ? PROVIDER_BASE_URL
    : builtinBaseUrl;
  const values = {
    aiProvider: AI_PROVIDER_RAW,
    aiProviderNormalized: AI_PROVIDER,
    providerName: providerDef?.providerName || "",
    providerBaseUrl: PROVIDER_BASE_URL,
    effectiveProviderBaseUrl,
    baseUrlIgnored: Boolean(
      providerDef && !requiresCustomBaseUrl && PROVIDER_BASE_URL,
    ),
    providerApiKey: PROVIDER_API_KEY,
  };

  if (!enabled) {
    return {
      enabled: false,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      providerMeta: null,
      error: null,
    };
  }

  if (AI_PROVIDER_RAW !== AI_PROVIDER) {
    return {
      enabled: true,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      providerMeta: null,
      error:
        `Invalid AI_PROVIDER: ${AI_PROVIDER_RAW}. ` +
        "AI_PROVIDER must be uppercase (for example: OPENAI, ANTHROPIC, CUSTOM_PROVIDER).",
    };
  }

  if (!providerDef) {
    return {
      enabled: true,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      providerMeta: null,
      error:
        `Invalid AI_PROVIDER: ${AI_PROVIDER_RAW}. ` +
        `Allowed values: ${VALID_ENV_AI_PROVIDERS.join(", ")}. ` +
        "Please update Railway Variables (AI_PROVIDER / PROVIDER_BASE_URL / PROVIDER_API_KEY).",
    };
  }

  if (requiresCustomBaseUrl) {
    if (!PROVIDER_BASE_URL) {
      return {
        enabled: true,
        valid: false,
        values,
        allowedProviders: VALID_ENV_AI_PROVIDERS,
        providerMeta: null,
        error:
          "PROVIDER_BASE_URL is required when AI_PROVIDER=CUSTOM_PROVIDER. " +
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
        providerMeta: null,
        error:
          `Invalid PROVIDER_BASE_URL: ${PROVIDER_BASE_URL}. ` +
          "Expected an absolute URL starting with http:// or https://. " +
          "Please update Railway Variables.",
      };
    }
  }

  if (!PROVIDER_API_KEY) {
    return {
      enabled: true,
      valid: false,
      values,
      allowedProviders: VALID_ENV_AI_PROVIDERS,
      providerMeta: null,
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
    providerMeta: {
      key: AI_PROVIDER,
      providerName: providerDef.providerName,
      selectedGroup: providerDef.selectedGroup,
      authChoice: providerDef.authChoice,
      customChoice: Boolean(providerDef.customChoice),
      requiresCustomBaseUrl,
      customApiType:
        providerDef.customApiType ||
        inferCustomApiTypeByProvider(providerDef.providerName),
      effectiveProviderBaseUrl,
    },
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

  const meta = envValidation.providerMeta;
  if (!meta) {
    return {
      ok: false,
      error: "Failed to resolve AI_PROVIDER metadata from environment variables.",
    };
  }

  const rawModel = typeof rawPayload.model === "string" ? rawPayload.model.trim() : "";
  const normalizedModel = normalizeEnvModel(rawModel, meta.providerName);

  const payload = {
    ...rawPayload,
    selectedGroup: meta.selectedGroup,
    authChoice: meta.authChoice,
    authSecret: PROVIDER_API_KEY,
    model: normalizedModel.model,
  };

  if (meta.customChoice) {
    return {
      ok: true,
      payload: {
        ...payload,
        customProviderName: meta.providerName,
        customBaseUrl: meta.effectiveProviderBaseUrl || "",
        customApiType: meta.customApiType,
        customModelId: normalizedModel.modelId,
      },
    };
  }

  return { ok: true, payload };
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
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.openai,
      options: [
        { value: "openai-api-key", label: "OpenAI API key" },
      ],
    },
    {
      value: "anthropic",
      label: "Anthropic",
      hint: "API key",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.anthropic,
      options: [
        { value: "apiKey", label: "Anthropic API key" },
      ],
    },
    {
      value: "google",
      label: "Google",
      hint: "API key",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.google,
      options: [
        { value: "gemini-api-key", label: "Google Gemini API key" },
      ],
    },
    {
      value: "openrouter",
      label: "OpenRouter",
      hint: "API key",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.openrouter,
      options: [{ value: "openrouter-api-key", label: "OpenRouter API key" }],
    },
    {
      value: "ai-gateway",
      label: "Vercel AI Gateway",
      hint: "API key",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS["ai-gateway"],
      options: [
        { value: "ai-gateway-api-key", label: "Vercel AI Gateway API key" },
      ],
    },
    {
      value: "moonshot",
      label: "Moonshot AI",
      hint: "Kimi K2 + Kimi Code",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.moonshot,
      options: [
        { value: "moonshot-api-key", label: "Moonshot AI API key" },
        { value: "kimi-code-api-key", label: "Kimi Code API key" },
      ],
    },
    {
      value: "zai",
      label: "Z.AI (GLM 4.7)",
      hint: "API key",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.zai,
      options: [{ value: "zai-api-key", label: "Z.AI (GLM 4.7) API key" }],
    },
    {
      value: "zai-cn",
      label: "Zhipu GLM API (CN / bigmodel.cn)",
      hint: "OpenAI-compatible API",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-cn"],
      custom: true,
      options: [{ value: "zai-cn", label: "Zhipu CN API Route" }],
      defaults: {
        customProviderName: "zai-cn",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-cn"],
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "zai-global",
      label: "Zhipu GLM API (Global / z.ai)",
      hint: "OpenAI-compatible API",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-global"],
      custom: true,
      options: [{ value: "zai-global", label: "Zhipu Global API Route" }],
      defaults: {
        customProviderName: "zai-global",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-global"],
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "zai-coding-cn",
      label: "Zhipu Coding API (CN / bigmodel.cn)",
      hint: "Coding-specific API",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-coding-cn"],
      custom: true,
      options: [{ value: "zai-coding-cn", label: "Zhipu Coding CN Route" }],
      defaults: {
        customProviderName: "zai-coding-cn",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-coding-cn"],
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "zai-coding-global",
      label: "Zhipu Coding API (Global / z.ai)",
      hint: "Coding-specific API",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-coding-global"],
      custom: true,
      options: [{ value: "zai-coding-global", label: "Zhipu Coding Global Route" }],
      defaults: {
        customProviderName: "zai-coding-global",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS["zai-coding-global"],
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "minimax",
      label: "MiniMax",
      hint: "M2.1 (recommended)",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.minimax,
      options: [
        { value: "minimax-api", label: "MiniMax M2.1" },
        { value: "minimax-api-lightning", label: "MiniMax M2.1 Lightning" },
      ],
    },
    {
      value: "qwen",
      label: "Qwen",
      hint: "OAuth",
      baseUrl: "",
      options: [{ value: "qwen-portal", label: "Qwen OAuth" }],
    },
    {
      value: "copilot",
      label: "Copilot",
      hint: "GitHub + local proxy",
      baseUrl: "",
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
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.synthetic || "",
      options: [{ value: "synthetic-api-key", label: "Synthetic API key" }],
    },
    {
      value: "opencode-zen",
      label: "OpenCode Zen",
      hint: "API key",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS["opencode-zen"] || "",
      options: [
        { value: "opencode-zen", label: "OpenCode Zen (multi-model proxy)" },
      ],
    },
    {
      value: "volcengine-plan",
      label: "VolcEngine Coding Plan (火山引擎)",
      hint: "Coding Plan API",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS["volcengine-plan"],
      custom: true,
      options: [
        { value: "volcengine-plan", label: "VolcEngine Coding Plan" },
      ],
      defaults: {
        customProviderName: "volcengine-plan",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS["volcengine-plan"],
        customApiType: "openai-completions",
        customModelId: "ark-code-latest",
      },
    },
    {
      value: "bedrock",
      label: "Amazon Bedrock",
      hint: "Anthropic via AWS",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.bedrock,
      custom: true,
      options: [
        { value: "bedrock", label: "Amazon Bedrock" },
      ],
      defaults: {
        customProviderName: "bedrock",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS.bedrock,
        customApiType: "anthropic-messages",
        customModelId: "",
      },
    },
    {
      value: "bailian",
      label: "Alibaba Bailian (阿里百炼)",
      hint: "DashScope API",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.bailian,
      custom: true,
      options: [
        { value: "bailian", label: "Alibaba Bailian" },
      ],
      defaults: {
        customProviderName: "bailian",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS.bailian,
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "ollama",
      label: "Ollama (Local)",
      hint: "Local models, no API key needed",
      baseUrl: DEFAULT_PROVIDER_BASE_URLS.ollama,
      custom: true,
      options: [
        { value: "ollama", label: "Ollama Local" },
      ],
      defaults: {
        customProviderName: "ollama",
        customBaseUrl: DEFAULT_PROVIDER_BASE_URLS.ollama,
        customApiType: "openai-completions",
        customModelId: "",
      },
    },
    {
      value: "custom-provider",
      label: "Custom Provider",
      hint: "OpenAI / Anthropic compatible",
      baseUrl: "",
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
    const envMeta = envProvider.providerMeta || null;
    authGroups.unshift({
      value: "provider-from-env",
      label: "ProviderFromEnv",
      hint: envProvider.valid
        ? `AI_PROVIDER=${envProvider.values.aiProviderNormalized || envProvider.values.aiProvider}`
        : "Invalid Railway Variables",
      baseUrl: envProvider.values.effectiveProviderBaseUrl || "",
      custom: true,
      fromEnv: true,
      options: [
        {
          value: "provider-from-env",
          label: "Use AI_PROVIDER / PROVIDER_API_KEY (PROVIDER_BASE_URL only for CUSTOM_PROVIDER)",
        },
      ],
      defaults: {
        customProviderName: envProvider.values.providerName || "",
        customBaseUrl: envProvider.values.effectiveProviderBaseUrl || "",
        customApiType:
          envMeta?.customApiType ||
          inferCustomApiTypeByProvider(envProvider.values.providerName),
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
  "zai-cn",
  "zai-global",
  "zai-coding-cn",
  "zai-coding-global",
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
  "zai-cn",
  "zai-global",
  "zai-coding-cn",
  "zai-coding-global",
  "volcengine-plan",
  "bedrock",
  "bailian",
  "ollama",
  "custom-provider",
];

function isCustomProvider(authChoice) {
  return CUSTOM_PROVIDER_CHOICES.includes(authChoice);
}

const HTTP_VALIDATION_SKIPPED_AUTH_CHOICES = new Set([
  "qwen-portal",
  "github-copilot",
  "copilot-proxy",
]);

function normalizeHttpBaseUrl(rawBaseUrl) {
  const value = String(rawBaseUrl || "").trim();
  if (!value) return "";
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      return "";
    }
    return url.toString().replace(/\/+$/, "");
  } catch {
    return "";
  }
}

function joinBaseUrl(baseUrl, pathSuffix) {
  const cleanBase = normalizeHttpBaseUrl(baseUrl);
  if (!cleanBase) return "";
  const baseWithSlash = `${cleanBase}/`;
  const cleanSuffix = String(pathSuffix || "").replace(/^\/+/, "");
  return new URL(cleanSuffix, baseWithSlash).toString();
}

function shortResponseBody(bodyText) {
  return String(bodyText || "").replace(/\s+/g, " ").trim().slice(0, 300);
}

function redactValidationUrl(url) {
  try {
    const u = new URL(url);
    if (u.searchParams.has("key")) {
      u.searchParams.set("key", "[REDACTED]");
    }
    return u.toString();
  } catch {
    return String(url || "");
  }
}

function inferProviderApiTypeForValidation(entry, providerName) {
  const customApiType = String(entry.customApiType || "").trim();
  if (customApiType) {
    if (customApiType === "anthropic") return "anthropic-messages";
    return customApiType;
  }

  const group = String(entry.selectedGroup || "").trim().toLowerCase();
  const authChoice = String(entry.authChoice || "").trim().toLowerCase();
  const provider = String(providerName || "").trim().toLowerCase();

  if (group === "google" || provider === "google" || authChoice === "gemini-api-key") {
    return "google-generative-ai";
  }

  if (
    group === "anthropic" ||
    provider === "anthropic" ||
    provider === "bedrock" ||
    authChoice === "apikey"
  ) {
    return "anthropic-messages";
  }

  return "openai-completions";
}

function resolveDefaultBaseUrlForProvider({ providerName, selectedGroup }) {
  const byProvider = DEFAULT_PROVIDER_BASE_URLS[String(providerName || "").trim().toLowerCase()];
  if (byProvider) return byProvider;
  const byGroup = DEFAULT_PROVIDER_BASE_URLS[String(selectedGroup || "").trim().toLowerCase()];
  if (byGroup) return byGroup;
  return "";
}

function resolveProviderValidationTarget(entry = {}) {
  const authChoice = String(entry.authChoice || "").trim();
  const selectedGroup = String(entry.selectedGroup || "").trim();
  const providerName = inferProviderName(entry);
  const baseUrlRaw =
    String(entry.customBaseUrl || "").trim() ||
    resolveDefaultBaseUrlForProvider({ providerName, selectedGroup });
  const baseUrl = normalizeHttpBaseUrl(baseUrlRaw);
  const apiType = inferProviderApiTypeForValidation(entry, providerName);
  const apiKey = String(entry.authSecret || "").trim();
  const model = String(entry.model || "").trim();

  return {
    authChoice,
    selectedGroup,
    providerName,
    baseUrl,
    apiType,
    apiKey,
    model,
  };
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 12_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });
    const bodyText = await response.text();
    return {
      ok: response.ok,
      status: response.status,
      statusText: response.statusText,
      bodyText,
      url,
      error: "",
    };
  } catch (err) {
    const isAbort = err?.name === "AbortError";
    return {
      ok: false,
      status: 0,
      statusText: "",
      bodyText: "",
      url,
      error: isAbort ? "request-timeout" : String(err?.message || err),
    };
  } finally {
    clearTimeout(timer);
  }
}

function buildValidationFailureResult({ target, endpoint, message, statusCode = 0, details = "" }) {
  return {
    valid: false,
    skipped: false,
    providerName: target.providerName,
    apiType: target.apiType,
    endpoint: endpoint ? redactValidationUrl(endpoint) : "",
    statusCode,
    message,
    details: details || "",
  };
}

function buildValidationSuccessResult({ target, endpoint, message, statusCode = 200, details = "" }) {
  return {
    valid: true,
    skipped: false,
    providerName: target.providerName,
    apiType: target.apiType,
    endpoint: endpoint ? redactValidationUrl(endpoint) : "",
    statusCode,
    message,
    details: details || "",
  };
}

async function probeOpenAiCompatibleProvider(target) {
  const candidates = [
    joinBaseUrl(target.baseUrl, "/models"),
    joinBaseUrl(target.baseUrl, "/v1/models"),
  ].filter(Boolean);
  const uniqueCandidates = [...new Set(candidates)];

  let lastResult = null;
  for (const endpoint of uniqueCandidates) {
    const result = await fetchWithTimeout(
      endpoint,
      {
        method: "GET",
        headers: {
          authorization: `Bearer ${target.apiKey}`,
          accept: "application/json",
        },
      },
      12_000,
    );
    lastResult = result;

    if (result.ok) {
      return buildValidationSuccessResult({
        target,
        endpoint,
        statusCode: result.status,
        message: "Provider endpoint and API key look valid.",
      });
    }

    if (result.status === 429) {
      return buildValidationSuccessResult({
        target,
        endpoint,
        statusCode: result.status,
        message: "Provider is reachable and credentials were accepted (rate limited).",
      });
    }

    if (result.status === 401 || result.status === 403) {
      return buildValidationFailureResult({
        target,
        endpoint,
        statusCode: result.status,
        message: "API key was rejected by provider.",
        details: shortResponseBody(result.bodyText),
      });
    }

    if (result.status === 404 || result.status === 405) {
      continue;
    }
  }

  return buildValidationFailureResult({
    target,
    endpoint: lastResult?.url || uniqueCandidates[0] || "",
    statusCode: lastResult?.status || 0,
    message: "Could not validate provider endpoint using OpenAI-compatible probe.",
    details: lastResult?.error || shortResponseBody(lastResult?.bodyText),
  });
}

async function probeAnthropicProvider(target) {
  const candidates = [
    joinBaseUrl(target.baseUrl, "/models"),
    joinBaseUrl(target.baseUrl, "/v1/models"),
  ].filter(Boolean);
  const uniqueCandidates = [...new Set(candidates)];

  let lastResult = null;
  for (const endpoint of uniqueCandidates) {
    const result = await fetchWithTimeout(
      endpoint,
      {
        method: "GET",
        headers: {
          "x-api-key": target.apiKey,
          "anthropic-version": "2023-06-01",
          accept: "application/json",
        },
      },
      12_000,
    );
    lastResult = result;

    if (result.ok) {
      return buildValidationSuccessResult({
        target,
        endpoint,
        statusCode: result.status,
        message: "Anthropic-compatible endpoint and API key look valid.",
      });
    }

    if (result.status === 429) {
      return buildValidationSuccessResult({
        target,
        endpoint,
        statusCode: result.status,
        message: "Provider is reachable and credentials were accepted (rate limited).",
      });
    }

    if (result.status === 401 || result.status === 403) {
      return buildValidationFailureResult({
        target,
        endpoint,
        statusCode: result.status,
        message: "API key was rejected by provider.",
        details: shortResponseBody(result.bodyText),
      });
    }

    if (result.status === 404 || result.status === 405) {
      continue;
    }
  }

  return buildValidationFailureResult({
    target,
    endpoint: lastResult?.url || uniqueCandidates[0] || "",
    statusCode: lastResult?.status || 0,
    message: "Could not validate provider endpoint using Anthropic-compatible probe.",
    details: lastResult?.error || shortResponseBody(lastResult?.bodyText),
  });
}

async function probeGoogleProvider(target) {
  const candidates = [
    joinBaseUrl(target.baseUrl, "/models"),
    joinBaseUrl(target.baseUrl, "/v1beta/models"),
  ].filter(Boolean);
  const uniqueCandidates = [...new Set(candidates)];

  let lastResult = null;
  for (const endpoint of uniqueCandidates) {
    const endpointUrl = new URL(endpoint);
    endpointUrl.searchParams.set("key", target.apiKey);
    const result = await fetchWithTimeout(
      endpointUrl.toString(),
      {
        method: "GET",
        headers: {
          accept: "application/json",
        },
      },
      12_000,
    );
    lastResult = result;

    if (result.ok) {
      return buildValidationSuccessResult({
        target,
        endpoint: endpointUrl.toString(),
        statusCode: result.status,
        message: "Google-compatible endpoint and API key look valid.",
      });
    }

    if (result.status === 429) {
      return buildValidationSuccessResult({
        target,
        endpoint: endpointUrl.toString(),
        statusCode: result.status,
        message: "Provider is reachable and credentials were accepted (rate limited).",
      });
    }

    if (result.status === 401 || result.status === 403) {
      return buildValidationFailureResult({
        target,
        endpoint: endpointUrl.toString(),
        statusCode: result.status,
        message: "API key was rejected by provider.",
        details: shortResponseBody(result.bodyText),
      });
    }

    if (result.status === 404 || result.status === 405) {
      continue;
    }
  }

  return buildValidationFailureResult({
    target,
    endpoint: lastResult?.url || uniqueCandidates[0] || "",
    statusCode: lastResult?.status || 0,
    message: "Could not validate provider endpoint using Google-compatible probe.",
    details: lastResult?.error || shortResponseBody(lastResult?.bodyText),
  });
}

async function validateProviderConnection(entry) {
  const target = resolveProviderValidationTarget(entry);

  if (!target.authChoice) {
    return {
      valid: false,
      skipped: false,
      providerName: target.providerName,
      apiType: target.apiType,
      endpoint: "",
      statusCode: 0,
      message: "Missing authChoice.",
      details: "",
    };
  }

  if (HTTP_VALIDATION_SKIPPED_AUTH_CHOICES.has(target.authChoice)) {
    return {
      valid: true,
      skipped: true,
      providerName: target.providerName,
      apiType: target.apiType,
      endpoint: "",
      statusCode: 0,
      message: `Validation skipped for ${target.authChoice}.`,
      details: "OAuth/local auth flow requires interactive validation.",
    };
  }

  if (!target.baseUrl) {
    return {
      valid: false,
      skipped: false,
      providerName: target.providerName,
      apiType: target.apiType,
      endpoint: "",
      statusCode: 0,
      message: "Cannot infer a valid provider base URL.",
      details:
        "Set custom base URL in setup, or choose a provider with a known default base URL.",
    };
  }

  if (!target.apiKey) {
    return {
      valid: false,
      skipped: false,
      providerName: target.providerName,
      apiType: target.apiType,
      endpoint: "",
      statusCode: 0,
      message: "Missing API key/token.",
      details: "",
    };
  }

  if (target.apiType === "anthropic-messages") {
    return probeAnthropicProvider(target);
  }

  if (target.apiType === "google-generative-ai") {
    return probeGoogleProvider(target);
  }

  return probeOpenAiCompatibleProvider(target);
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

function normalizeSetupPayload(rawPayload = {}) {
  let payload = rawPayload;

  if (Array.isArray(rawPayload.providers) && rawPayload.providers.length > 0) {
    const transformedProviders = [];
    for (const provider of rawPayload.providers) {
      const p = provider || {};
      if (p.authChoice === "provider-from-env") {
        const transformed = buildPayloadFromEnvProvider(p);
        if (!transformed.ok) {
          return { ok: false, error: transformed.error };
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
      return { ok: false, error: transformed.error };
    }
    payload = transformed.payload;
  }

  return { ok: true, payload };
}

function buildProviderEntries(payload) {
  return Array.isArray(payload.providers) && payload.providers.length > 0
    ? payload.providers.map((p, idx) => ({
        ...p,
        id: String(p.id || `provider-${idx + 1}`),
        model: String(p.model || "").trim(),
      }))
    : [{ ...payload, id: "provider-1", model: String(payload.model || "").trim() }];
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function ensureObjectField(obj, key) {
  if (!isPlainObject(obj[key])) {
    obj[key] = {};
  }
  return obj[key];
}

function normalizeText(value) {
  return String(value || "").replace(/\r\n/g, "\n");
}

function jsonText(value) {
  return `${JSON.stringify(value, null, 2)}\n`;
}

function parseJsonObjectText(text) {
  const raw = String(text || "").trim();
  if (!raw) return { value: {}, error: null };
  try {
    const parsed = JSON.parse(raw);
    if (!isPlainObject(parsed)) {
      return { value: {}, error: "json-root-is-not-object" };
    }
    return { value: parsed, error: null };
  } catch (err) {
    return { value: {}, error: err.message || "json-parse-error" };
  }
}

function buildUnifiedLineDiff(beforeText, afterText) {
  const beforeLines = normalizeText(beforeText).split("\n");
  const afterLines = normalizeText(afterText).split("\n");
  const complexity = beforeLines.length * afterLines.length;
  if (complexity > 250_000) {
    return [
      "--- current",
      "+++ preview",
      `@@ large file: ${beforeLines.length} -> ${afterLines.length} lines @@`,
      "[diff omitted because content is too large]",
    ].join("\n");
  }

  const dp = Array.from({ length: beforeLines.length + 1 }, () =>
    Array(afterLines.length + 1).fill(0),
  );

  for (let i = 1; i <= beforeLines.length; i += 1) {
    for (let j = 1; j <= afterLines.length; j += 1) {
      if (beforeLines[i - 1] === afterLines[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1] + 1;
      } else {
        dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
      }
    }
  }

  const ops = [];
  let i = beforeLines.length;
  let j = afterLines.length;

  while (i > 0 && j > 0) {
    if (beforeLines[i - 1] === afterLines[j - 1]) {
      ops.push({ type: " ", line: beforeLines[i - 1] });
      i -= 1;
      j -= 1;
      continue;
    }
    if (dp[i - 1][j] >= dp[i][j - 1]) {
      ops.push({ type: "-", line: beforeLines[i - 1] });
      i -= 1;
    } else {
      ops.push({ type: "+", line: afterLines[j - 1] });
      j -= 1;
    }
  }
  while (i > 0) {
    ops.push({ type: "-", line: beforeLines[i - 1] });
    i -= 1;
  }
  while (j > 0) {
    ops.push({ type: "+", line: afterLines[j - 1] });
    j -= 1;
  }

  ops.reverse();
  const lines = ["--- current", "+++ preview"];
  for (const op of ops) {
    lines.push(`${op.type}${op.line}`);
  }
  return lines.join("\n");
}

function buildFilePreviewEntry({ filePath, beforeText, afterText, reason }) {
  const before = normalizeText(beforeText ?? "");
  const after = normalizeText(afterText ?? "");
  const changed = before !== after;
  return {
    path: filePath,
    reason,
    existsBefore: beforeText != null,
    changed,
    diff: changed ? buildUnifiedLineDiff(before, after) : "",
    before,
    after,
  };
}

function deriveProviderPreviewMeta(entry = {}) {
  const apiKey = String(entry.authSecret || "").trim();
  if (isCustomProvider(entry.authChoice)) {
    const providerName =
      entry.authChoice === "custom-provider"
        ? (entry.customProviderName || "custom").trim() || "custom"
        : String(entry.authChoice || "").trim() || "custom";
    const rawApiType = (entry.customApiType || "openai-completions").trim();
    const apiType =
      rawApiType === "anthropic" ? "anthropic-messages" : rawApiType;
    return {
      isCustom: true,
      providerName,
      profileId: `${providerName}:default`,
      apiKey,
      baseUrl: String(entry.customBaseUrl || "").trim(),
      apiType,
      modelId: String(entry.customModelId || "").trim(),
    };
  }

  const providerName = inferProviderName(entry);
  return {
    isCustom: false,
    providerName,
    profileId: `${providerName}:default`,
    apiKey,
    baseUrl: "",
    apiType: "",
    modelId: "",
  };
}

function applyGatewayPreviewToConfig(cfg) {
  const gateway = ensureObjectField(cfg, "gateway");
  const controlUi = ensureObjectField(gateway, "controlUi");
  const auth = ensureObjectField(gateway, "auth");
  controlUi.allowInsecureAuth = true;
  auth.token = OPENCLAW_GATEWAY_TOKEN;
  gateway.trustedProxies = ["127.0.0.1"];
}

function applyProviderPreviewToConfig(cfg, providerEntries) {
  const authUpdates = [];
  let hasCustomProvider = false;

  for (const provider of providerEntries) {
    const meta = deriveProviderPreviewMeta(provider);

    if (meta.isCustom) {
      hasCustomProvider = true;
      const models = ensureObjectField(cfg, "models");
      const providers = ensureObjectField(models, "providers");
      const providerCfg = { api: meta.apiType };
      if (meta.baseUrl) providerCfg.baseUrl = meta.baseUrl;
      if (meta.apiKey) providerCfg.apiKey = meta.apiKey;
      if (meta.modelId) {
        providerCfg.models = [{ id: meta.modelId, name: meta.modelId }];
      }
      providers[meta.providerName] = providerCfg;

      if (isPlainObject(cfg.auth)) {
        if (
          isPlainObject(cfg.auth.profiles) &&
          cfg.auth.profiles["openai:default"] !== undefined
        ) {
          delete cfg.auth.profiles["openai:default"];
        }
        if (isPlainObject(cfg.auth.order) && cfg.auth.order.openai !== undefined) {
          delete cfg.auth.order.openai;
        }
      }
    }

    if (meta.apiKey) {
      const auth = ensureObjectField(cfg, "auth");
      const profiles = ensureObjectField(auth, "profiles");
      const order = ensureObjectField(auth, "order");
      profiles[meta.profileId] = {
        provider: meta.providerName,
        mode: "api_key",
      };
      order[meta.providerName] = [meta.profileId];
      authUpdates.push({
        providerName: meta.providerName,
        profileId: meta.profileId,
        apiKey: meta.apiKey,
      });
    }
  }

  return { authUpdates, hasCustomProvider };
}

function applyAgentsPreviewToConfig(cfg, agents) {
  const agentsRoot = ensureObjectField(cfg, "agents");
  const defaults = ensureObjectField(agentsRoot, "defaults");
  const previousModels = isPlainObject(defaults.models) ? defaults.models : {};

  const nextModelMap = {};
  for (const agent of agents) {
    nextModelMap[agent.name] = agent.model;
  }

  let primaryModel = String(nextModelMap.primary || "").trim();
  if (!primaryModel && agents[0]) {
    primaryModel = String(agents[0].model || "").trim();
    if (primaryModel) nextModelMap.primary = primaryModel;
  }

  const nextModels = {};
  for (const model of new Set(Object.values(nextModelMap).filter(Boolean))) {
    const existing = previousModels[model];
    nextModels[model] =
      existing && isPlainObject(existing)
        ? JSON.parse(JSON.stringify(existing))
        : {};
  }

  defaults.model = nextModelMap;
  defaults.models = nextModels;
  return { primaryModel };
}

function applyChannelsPreviewToConfig(cfg, payload) {
  let channels = null;
  function getChannels() {
    if (!channels) {
      channels = ensureObjectField(cfg, "channels");
    }
    return channels;
  }
  if (payload.telegramToken?.trim()) {
    getChannels().telegram = {
      enabled: true,
      dmPolicy: "pairing",
      botToken: payload.telegramToken.trim(),
      groupPolicy: "allowlist",
      streamMode: "partial",
    };
  }

  if (payload.discordToken?.trim()) {
    getChannels().discord = {
      enabled: true,
      token: payload.discordToken.trim(),
      groupPolicy: "allowlist",
      dm: { policy: "pairing" },
    };
  }

  if (payload.slackBotToken?.trim() || payload.slackAppToken?.trim()) {
    getChannels().slack = {
      enabled: true,
      botToken: payload.slackBotToken?.trim() || undefined,
      appToken: payload.slackAppToken?.trim() || undefined,
    };
  }
}

function simulateAuthProfileStore(beforeStore, authUpdates) {
  const store =
    beforeStore && isPlainObject(beforeStore)
      ? JSON.parse(JSON.stringify(beforeStore))
      : {};
  let changed = false;

  if (!isPlainObject(store.profiles)) {
    store.profiles = {};
  }

  for (const [id, profile] of Object.entries(store.profiles)) {
    const p = profile || {};
    const pKey = typeof p.key === "string" ? p.key : "";
    const isPlaceholder =
      pKey.includes(CUSTOM_PROVIDER_BOOTSTRAP_OPENAI_KEY) ||
      pKey.includes("placeholder-for-custom-provider");
    if (id === "openai:default" || isPlaceholder) {
      delete store.profiles[id];
      changed = true;
    }
  }

  for (const update of authUpdates) {
    if (!update.apiKey) continue;
    const nextProfile = {
      type: "api_key",
      provider: update.providerName,
      key: update.apiKey,
    };
    const previous = store.profiles[update.profileId];
    if (JSON.stringify(previous) !== JSON.stringify(nextProfile)) {
      store.profiles[update.profileId] = nextProfile;
      changed = true;
    }
  }

  if (isPlainObject(store.usageStats)) {
    for (const usageId of Object.keys(store.usageStats)) {
      if (!store.profiles[usageId]) {
        delete store.usageStats[usageId];
        changed = true;
      }
    }
  }

  return { store, changed };
}

function previewSanitizeBootstrapOpenAIEnvContent(beforeText) {
  const original = normalizeText(beforeText);
  const lines = original.split(/\n/);
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

  const after = `${kept.join("\n").replace(/\n+$/, "")}\n`;
  return { changed: removed > 0, removed, afterText: after };
}

function generateSetupPreview(payload) {
  const notes = [
    "Preview is based on wrapper-level config logic; openclaw onboard may add extra defaults.",
    "No files are modified by this preview.",
  ];
  const files = [];
  const cfgPath = configPath();
  const providerEntries = buildProviderEntries(payload);
  const normalizedAgents = normalizeAgentsFromPayload(payload.agents, providerEntries);

  let cfgBeforeText = null;
  let cfgBeforeObj = {};
  if (fs.existsSync(cfgPath)) {
    try {
      cfgBeforeText = fs.readFileSync(cfgPath, "utf8");
      const parsed = parseJsonObjectText(cfgBeforeText);
      cfgBeforeObj = parsed.value;
      if (parsed.error) {
        notes.push(`Warning: could not parse existing config ${cfgPath}: ${parsed.error}`);
      }
    } catch (err) {
      notes.push(`Warning: could not read config ${cfgPath}: ${err.message}`);
    }
  }

  const cfgPreview = JSON.parse(JSON.stringify(cfgBeforeObj || {}));
  applyGatewayPreviewToConfig(cfgPreview);
  const providerPreview = applyProviderPreviewToConfig(cfgPreview, providerEntries);
  applyAgentsPreviewToConfig(cfgPreview, normalizedAgents);
  applyChannelsPreviewToConfig(cfgPreview, payload);

  const cfgFilePreview = buildFilePreviewEntry({
    filePath: cfgPath,
    beforeText: cfgBeforeText,
    afterText: jsonText(cfgPreview),
    reason: "openclaw main config",
  });
  if (cfgFilePreview.changed) files.push(cfgFilePreview);

  const authStorePaths = listAuthProfileStorePaths();
  for (const storePath of authStorePaths) {
    let beforeText = null;
    let beforeStore = {};

    if (fs.existsSync(storePath)) {
      try {
        beforeText = fs.readFileSync(storePath, "utf8");
        const parsed = parseJsonObjectText(beforeText);
        beforeStore = parsed.value;
        if (parsed.error) {
          notes.push(`Warning: could not parse auth store ${storePath}: ${parsed.error}`);
        }
      } catch (err) {
        notes.push(`Warning: could not read auth store ${storePath}: ${err.message}`);
      }
    }

    const simulated = simulateAuthProfileStore(beforeStore, providerPreview.authUpdates);
    if (!simulated.changed) continue;

    const storePreview = buildFilePreviewEntry({
      filePath: storePath,
      beforeText,
      afterText: jsonText(simulated.store),
      reason: "auth profile store",
    });
    if (storePreview.changed) files.push(storePreview);
  }

  if (providerPreview.hasCustomProvider) {
    const envPath = path.join(STATE_DIR, ".env");
    if (fs.existsSync(envPath)) {
      try {
        const envBefore = fs.readFileSync(envPath, "utf8");
        const envPreview = previewSanitizeBootstrapOpenAIEnvContent(envBefore);
        if (envPreview.changed) {
          const envFilePreview = buildFilePreviewEntry({
            filePath: envPath,
            beforeText: envBefore,
            afterText: envPreview.afterText,
            reason: "bootstrap OPENAI_API_KEY cleanup",
          });
          if (envFilePreview.changed) files.push(envFilePreview);
        }
      } catch (err) {
        notes.push(`Warning: could not read env file ${envPath}: ${err.message}`);
      }
    }
  }

  return {
    notes,
    files,
    summary: {
      providers: providerEntries.length,
      agents: normalizedAgents.length,
      changedFiles: files.length,
      channels: [
        payload.telegramToken?.trim() ? "telegram" : "",
        payload.discordToken?.trim() ? "discord" : "",
        payload.slackBotToken?.trim() || payload.slackAppToken?.trim() ? "slack" : "",
      ].filter(Boolean),
    },
  };
}

app.post("/setup/api/preview", requireSetupAuth, async (req, res) => {
  try {
    const { version } = await getOpenclawInfo();
    if (isConfigured()) {
      return res.json({
        ok: true,
        configured: true,
        blocked: true,
        openclawVersion: version,
        notes: [
          "Instance is already configured. /setup/api/run will not apply setup again unless you reset first.",
        ],
        files: [],
        summary: { providers: 0, agents: 0, changedFiles: 0, channels: [] },
      });
    }

    const normalized = normalizeSetupPayload(req.body || {});
    if (!normalized.ok) {
      return res.status(400).json({ ok: false, error: normalized.error });
    }

    const payload = normalized.payload;
    const validationError = validatePayload(payload);
    if (validationError) {
      return res.status(400).json({ ok: false, error: validationError });
    }

    const preview = generateSetupPreview(payload);
    return res.json({
      ok: true,
      configured: false,
      blocked: false,
      openclawVersion: version,
      generatedAt: new Date().toISOString(),
      ...preview,
    });
  } catch (err) {
    console.error("[/setup/api/preview] error:", err);
    return res
      .status(500)
      .json({ ok: false, error: `Internal error: ${String(err)}` });
  }
});

app.post("/setup/api/provider/validate", requireSetupAuth, async (req, res) => {
  try {
    const rawBody = req.body || {};
    const rawProvider =
      rawBody.provider &&
      typeof rawBody.provider === "object" &&
      !Array.isArray(rawBody.provider)
        ? rawBody.provider
        : rawBody;

    if (!rawProvider || typeof rawProvider !== "object" || Array.isArray(rawProvider)) {
      return res.status(400).json({
        ok: false,
        valid: false,
        error: "Invalid request body: provider object is required.",
      });
    }

    const normalized = normalizeSetupPayload({ providers: [rawProvider] });
    if (!normalized.ok) {
      return res.status(400).json({
        ok: false,
        valid: false,
        error: normalized.error,
      });
    }

    const providerEntry = buildProviderEntries(normalized.payload)[0] || {};
    const providerValidationError = validatePayload(providerEntry);
    if (providerValidationError) {
      return res.status(400).json({
        ok: false,
        valid: false,
        error: providerValidationError,
      });
    }

    const result = await validateProviderConnection(providerEntry);
    const statusCode = result.valid ? 200 : 400;
    return res.status(statusCode).json({
      ok: result.valid,
      ...result,
    });
  } catch (err) {
    console.error("[/setup/api/provider/validate] error:", err);
    return res.status(500).json({
      ok: false,
      valid: false,
      error: `Internal error: ${String(err)}`,
    });
  }
});

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

    const normalized = normalizeSetupPayload(req.body || {});
    if (!normalized.ok) {
      return res.status(400).json({ ok: false, output: normalized.error });
    }
    const payload = normalized.payload;

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

      const providerEntries = buildProviderEntries(payload);
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
