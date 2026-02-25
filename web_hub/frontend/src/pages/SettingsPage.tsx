import { useState, useEffect, useCallback } from "react";
import { useAuth } from "@/context/AuthContext";
import api from "@/lib/api";
import {
  Save,
  Loader2,
  CheckCircle,
  AlertTriangle,
  Brain,
  Shield,
  Zap,
  RefreshCw,
  Lock,
  ChevronDown,
} from "lucide-react";

interface BotSummary {
  bot_id: string;
  bot_type: string;
  guild_id: string | null;
  is_active: boolean;
  ai_model_id: string | null;
  feature_flags: Record<string, boolean>;
}

interface BotConfig {
  bot_id: string;
  guild_id: string | null;
  ai_system_prompt: string | null;
  ai_model_id: string | null;
  log_channel_id: string | null;
  feature_flags: Record<string, boolean>;
  moderation_config: Record<string, unknown>;
}

interface FeatureFlagMeta {
  key: string;
  label: string;
  description: string;
  icon: React.ComponentType<{ className?: string }>;
}

const FLAG_DEFINITIONS: FeatureFlagMeta[] = [
  {
    key: "moderation_enabled",
    label: "Moderation System",
    description: "Enable the Apostle moderation pipeline (regex + AI)",
    icon: Shield,
  },
  {
    key: "ai_moderation_enabled",
    label: "AI Deep Analysis",
    description: "Enable DeepSeek/OpenRouter semantic analysis for messages",
    icon: Brain,
  },
  {
    key: "leveling_enabled",
    label: "XP & Leveling",
    description: "Enable the Two Paths XP accumulation and level-up system",
    icon: Zap,
  },
  {
    key: "voice_xp_enabled",
    label: "Voice XP",
    description: "Grant XP for time spent in voice channels",
    icon: Zap,
  },
  {
    key: "announce_levelup",
    label: "Level-Up Announcements",
    description: "Post a message when a user levels up",
    icon: Zap,
  },
];

function Toggle({
  enabled,
  onChange,
  disabled,
}: {
  enabled: boolean;
  onChange: (val: boolean) => void;
  disabled: boolean;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={enabled}
      disabled={disabled}
      onClick={() => onChange(!enabled)}
      className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-tm-accent/40 focus:ring-offset-2 focus:ring-offset-tm-bg disabled:opacity-30 disabled:cursor-not-allowed ${
        enabled ? "bg-tm-accent" : "bg-white/10"
      }`}
    >
      <span
        className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform duration-200 ${
          enabled ? "translate-x-6" : "translate-x-1"
        }`}
      />
    </button>
  );
}

export default function SettingsPage() {
  const { hasClearance } = useAuth();
  const canEdit = hasClearance("owner", "admin");

  const [bots, setBots] = useState<BotSummary[]>([]);
  const [selectedBotId, setSelectedBotId] = useState<string | null>(null);
  const [config, setConfig] = useState<BotConfig | null>(null);

  const [prompt, setPrompt] = useState("");
  const [modelId, setModelId] = useState("");
  const [logChannelId, setLogChannelId] = useState("");
  const [flags, setFlags] = useState<Record<string, boolean>>({});

  const [isLoadingBots, setIsLoadingBots] = useState(true);
  const [isLoadingConfig, setIsLoadingConfig] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [toast, setToast] = useState<{
    message: string;
    type: "success" | "error";
    subscribers?: number;
  } | null>(null);
  const [hasChanges, setHasChanges] = useState(false);

  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 5000);
    return () => clearTimeout(t);
  }, [toast]);

  useEffect(() => {
    (async () => {
      try {
        const res = await api.get<{ bots: BotSummary[] }>("/api/config/bots");
        setBots(res.data.bots);
        if (res.data.bots.length > 0 && !selectedBotId) {
          setSelectedBotId(res.data.bots[0].bot_id);
        }
      } catch {
        setToast({ message: "Failed to load bot list", type: "error" });
      } finally {
        setIsLoadingBots(false);
      }
    })();
  }, []);

  const loadConfig = useCallback(async (botId: string) => {
    setIsLoadingConfig(true);
    try {
      const res = await api.get<BotConfig>(`/api/config/bot/${botId}`);
      const c = res.data;
      setConfig(c);
      setPrompt(c.ai_system_prompt ?? "");
      setModelId(c.ai_model_id ?? "");
      setLogChannelId(c.log_channel_id ?? "");
      setFlags(c.feature_flags ?? {});
      setHasChanges(false);
    } catch {
      setToast({ message: "Failed to load bot configuration", type: "error" });
      setConfig(null);
    } finally {
      setIsLoadingConfig(false);
    }
  }, []);

  useEffect(() => {
    if (selectedBotId) {
      loadConfig(selectedBotId);
    }
  }, [selectedBotId, loadConfig]);

  const handleFlagChange = (key: string, value: boolean) => {
    setFlags((prev) => ({ ...prev, [key]: value }));
    setHasChanges(true);
  };

  const handleSave = async () => {
    if (!selectedBotId || !canEdit) return;
    setIsSaving(true);

    const payload: Record<string, unknown> = {};

    if (prompt !== (config?.ai_system_prompt ?? "")) {
      payload.ai_system_prompt = prompt;
    }
    if (modelId !== (config?.ai_model_id ?? "")) {
      payload.ai_model_id = modelId;
    }
    if (logChannelId !== (config?.log_channel_id ?? "")) {
      payload.log_channel_id = logChannelId;
    }

    const flagsChanged =
      JSON.stringify(flags) !== JSON.stringify(config?.feature_flags ?? {});
    if (flagsChanged) {
      payload.feature_flags = flags;
    }

    if (Object.keys(payload).length === 0) {
      setToast({ message: "No changes to save", type: "error" });
      setIsSaving(false);
      return;
    }

    try {
      const res = await api.put<BotConfig>(
        `/api/config/bot/${selectedBotId}`,
        payload
      );
      setConfig(res.data);
      setHasChanges(false);

      setToast({
        message: "Configuration saved — hot-reload signal sent",
        type: "success",
      });
    } catch {
      setToast({ message: "Failed to save configuration", type: "error" });
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoadingBots) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-tm-accent animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">
      {toast && (
        <div
          className={`fixed top-6 right-6 z-50 px-5 py-3 rounded-xl border text-sm font-medium shadow-glass animate-fade-in flex items-center gap-2 ${
            toast.type === "success"
              ? "bg-tm-success/10 border-tm-success/20 text-tm-success"
              : "bg-tm-danger/10 border-tm-danger/20 text-tm-danger"
          }`}
        >
          {toast.type === "success" ? (
            <CheckCircle className="w-4 h-4 flex-shrink-0" />
          ) : (
            <AlertTriangle className="w-4 h-4 flex-shrink-0" />
          )}
          {toast.message}
        </div>
      )}

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-tm-text">Settings</h1>
          <p className="text-sm text-tm-muted mt-1">
            Bot configuration and feature flags
          </p>
        </div>
        <div className="flex items-center gap-3">
          {selectedBotId && (
            <button
              onClick={() => loadConfig(selectedBotId)}
              disabled={isLoadingConfig}
              className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm text-tm-muted hover:text-tm-text bg-white/[0.03] hover:bg-white/[0.06] border border-tm-border transition-all duration-200"
            >
              <RefreshCw
                className={`w-4 h-4 ${isLoadingConfig ? "animate-spin" : ""}`}
              />
              Reload
            </button>
          )}
        </div>
      </div>

      {!canEdit && (
        <div className="flex items-center gap-3 px-5 py-4 rounded-2xl border bg-tm-warning/5 border-tm-warning/20">
          <Lock className="w-5 h-5 text-tm-warning flex-shrink-0" />
          <div>
            <p className="text-sm font-medium text-tm-warning">Read-Only Mode</p>
            <p className="text-xs text-tm-muted mt-0.5">
              Your clearance level does not permit configuration changes
            </p>
          </div>
        </div>
      )}

      <div className="relative">
        <select
          value={selectedBotId ?? ""}
          onChange={(e) => setSelectedBotId(e.target.value)}
          className="w-full appearance-none bg-tm-surface border border-tm-border rounded-xl px-4 py-3 text-sm text-tm-text focus:outline-none focus:ring-2 focus:ring-tm-accent/40 focus:border-tm-accent/40 pr-10"
        >
          {bots.map((bot) => (
            <option key={bot.bot_id} value={bot.bot_id}>
              {bot.bot_type} — {bot.bot_id.slice(0, 8)}...{bot.bot_id.slice(-4)}
              {bot.is_active ? " (active)" : " (inactive)"}
            </option>
          ))}
        </select>
        <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 text-tm-muted pointer-events-none" />
      </div>

      {isLoadingConfig ? (
        <div className="flex items-center justify-center h-40">
          <Loader2 className="w-6 h-6 text-tm-accent animate-spin" />
        </div>
      ) : config ? (
        <div className="space-y-6">
          <div className="bg-glass-gradient backdrop-blur-glass border border-tm-border shadow-glass rounded-2xl p-6 space-y-4">
            <div className="flex items-center gap-2 mb-2">
              <Brain className="w-5 h-5 text-tm-accent" />
              <h3 className="text-base font-semibold text-tm-text">
                AI System Prompt
              </h3>
            </div>
            <textarea
              value={prompt}
              onChange={(e) => {
                setPrompt(e.target.value);
                setHasChanges(true);
              }}
              disabled={!canEdit}
              rows={10}
              placeholder="Enter the system prompt for the Apostle AI moderation engine..."
              className="w-full bg-tm-bg/60 border border-tm-border rounded-xl px-4 py-3 text-sm text-tm-text placeholder:text-tm-muted/40 font-mono leading-relaxed resize-y focus:outline-none focus:ring-2 focus:ring-tm-accent/40 focus:border-tm-accent/40 disabled:opacity-50 disabled:cursor-not-allowed"
            />
            <div className="flex items-center gap-4">
              <div className="flex-1">
                <label className="block text-xs text-tm-muted mb-1.5">
                  AI Model
                </label>
                <input
                  type="text"
                  value={modelId}
                  onChange={(e) => {
                    setModelId(e.target.value);
                    setHasChanges(true);
                  }}
                  disabled={!canEdit}
                  placeholder="deepseek/deepseek-chat"
                  className="w-full bg-tm-bg/60 border border-tm-border rounded-lg px-3 py-2 text-sm text-tm-text font-mono placeholder:text-tm-muted/40 focus:outline-none focus:ring-2 focus:ring-tm-accent/40 disabled:opacity-50 disabled:cursor-not-allowed"
                />
              </div>
              <div className="flex-1">
                <label className="block text-xs text-tm-muted mb-1.5">
                  Log Channel ID
                </label>
                <input
                  type="text"
                  value={logChannelId}
                  onChange={(e) => {
                    setLogChannelId(e.target.value);
                    setHasChanges(true);
                  }}
                  disabled={!canEdit}
                  placeholder="123456789012345678"
                  className="w-full bg-tm-bg/60 border border-tm-border rounded-lg px-3 py-2 text-sm text-tm-text font-mono placeholder:text-tm-muted/40 focus:outline-none focus:ring-2 focus:ring-tm-accent/40 disabled:opacity-50 disabled:cursor-not-allowed"
                />
              </div>
            </div>
          </div>

          <div className="bg-glass-gradient backdrop-blur-glass border border-tm-border shadow-glass rounded-2xl p-6">
            <div className="flex items-center gap-2 mb-5">
              <Zap className="w-5 h-5 text-tm-accent" />
              <h3 className="text-base font-semibold text-tm-text">
                Feature Flags
              </h3>
            </div>
            <div className="space-y-1">
              {FLAG_DEFINITIONS.map((def) => {
                const Icon = def.icon;
                const isEnabled = flags[def.key] ?? true;
                return (
                  <div
                    key={def.key}
                    className="flex items-center justify-between py-3 px-4 rounded-xl hover:bg-white/[0.02] transition-colors duration-150"
                  >
                    <div className="flex items-center gap-3">
                      <Icon className="w-4 h-4 text-tm-muted flex-shrink-0" />
                      <div>
                        <p className="text-sm font-medium text-tm-text">
                          {def.label}
                        </p>
                        <p className="text-xs text-tm-muted mt-0.5">
                          {def.description}
                        </p>
                      </div>
                    </div>
                    <Toggle
                      enabled={isEnabled}
                      onChange={(val) => handleFlagChange(def.key, val)}
                      disabled={!canEdit}
                    />
                  </div>
                );
              })}
            </div>
          </div>

          {canEdit && (
            <div className="flex items-center justify-between pt-2">
              <div className="text-xs text-tm-muted">
                {hasChanges ? (
                  <span className="text-tm-warning">Unsaved changes</span>
                ) : (
                  <span>All changes saved</span>
                )}
              </div>
              <button
                onClick={handleSave}
                disabled={isSaving || !hasChanges}
                className="flex items-center gap-2 px-6 py-2.5 rounded-xl text-sm font-medium bg-tm-accent hover:bg-tm-accent-dim text-white transition-all duration-200 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {isSaving ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Save className="w-4 h-4" />
                )}
                {isSaving ? "Saving..." : "Save & Hot-Reload"}
              </button>
            </div>
          )}
        </div>
      ) : (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <AlertTriangle className="w-10 h-10 text-tm-muted/30 mb-4" />
          <p className="text-tm-muted text-sm">
            No configuration found for this bot
          </p>
        </div>
      )}
    </div>
  );
}
