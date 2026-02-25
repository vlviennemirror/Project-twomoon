import { useState, useEffect, useRef, useCallback } from "react";
import { useAuth } from "@/context/AuthContext";
import api from "@/lib/api";
import {
  Server,
  Play,
  Square,
  RotateCcw,
  Wifi,
  WifiOff,
  Clock,
  AlertTriangle,
  Loader2,
  RefreshCw,
} from "lucide-react";

interface AgentHealth {
  agent_online: boolean;
  last_seen_seconds_ago?: number;
}

interface BotEntry {
  bot_id: string;
  status: string;
  pid?: number;
  uptime_seconds?: number;
  exit_code?: number | null;
  memory_mb?: number;
}

interface FleetResponse {
  agent: AgentHealth;
  fleet_size: number;
  bots: BotEntry[];
}

const STATUS_STYLES: Record<string, { dot: string; label: string; bg: string }> = {
  RUNNING: {
    dot: "bg-tm-success",
    label: "text-tm-success",
    bg: "bg-tm-success/5 border-tm-success/20",
  },
  STARTING: {
    dot: "bg-tm-warning animate-pulse",
    label: "text-tm-warning",
    bg: "bg-tm-warning/5 border-tm-warning/20",
  },
  STOPPING: {
    dot: "bg-tm-warning animate-pulse",
    label: "text-tm-warning",
    bg: "bg-tm-warning/5 border-tm-warning/20",
  },
  CRASHED: {
    dot: "bg-tm-danger",
    label: "text-tm-danger",
    bg: "bg-tm-danger/5 border-tm-danger/20",
  },
  EXITED: {
    dot: "bg-tm-muted",
    label: "text-tm-muted",
    bg: "bg-white/[0.02] border-tm-border",
  },
  UNKNOWN: {
    dot: "bg-tm-muted animate-pulse",
    label: "text-tm-muted",
    bg: "bg-white/[0.02] border-tm-border",
  },
  NOT_RUNNING: {
    dot: "bg-tm-muted",
    label: "text-tm-muted",
    bg: "bg-white/[0.02] border-tm-border",
  },
};

function formatUptime(seconds?: number): string {
  if (!seconds || seconds < 0) return "--";
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function truncateId(id: string): string {
  if (id.length <= 12) return id;
  return `${id.slice(0, 8)}...${id.slice(-4)}`;
}

export default function FleetPage() {
  const { hasClearance } = useAuth();
  const canControl = hasClearance("owner", "admin");

  const [data, setData] = useState<FleetResponse | null>(null);
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [toast, setToast] = useState<{
    message: string;
    type: "success" | "error";
  } | null>(null);

  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const fetchFleet = useCallback(async (isBackground = false) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const response = await api.get<FleetResponse>("/api/fleet/status", {
        signal: controller.signal,
      });
      setData(response.data);
      setError(null);
    } catch (err: unknown) {
      if (err instanceof Error && err.name === "CanceledError") return;
      if (!isBackground) {
        setError("Failed to fetch fleet status");
      }
    } finally {
      setIsInitialLoad(false);
    }
  }, []);

  useEffect(() => {
    fetchFleet(false);
    intervalRef.current = setInterval(() => fetchFleet(true), 5000);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
      abortRef.current?.abort();
    };
  }, [fetchFleet]);

  useEffect(() => {
    if (!toast) return;
    const timer = setTimeout(() => setToast(null), 4000);
    return () => clearTimeout(timer);
  }, [toast]);

  const handleAction = async (
    botId: string,
    action: "start" | "stop" | "restart"
  ) => {
    const actionKey = `${botId}-${action}`;
    setPendingAction(actionKey);

    try {
      await api.post(`/api/fleet/${botId}/${action}`);
      setToast({
        message: `${action.charAt(0).toUpperCase() + action.slice(1)} command sent`,
        type: "success",
      });
      setTimeout(() => fetchFleet(true), 1000);
    } catch {
      setToast({ message: `Failed to ${action} bot`, type: "error" });
    } finally {
      setPendingAction(null);
    }
  };

  if (isInitialLoad) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-tm-accent animate-spin" />
      </div>
    );
  }

  const agent = data?.agent ?? { agent_online: false };
  const bots = data?.bots ?? [];

  return (
    <div className="space-y-6 animate-fade-in">
      {toast && (
        <div
          className={`fixed top-6 right-6 z-50 px-4 py-3 rounded-xl border text-sm font-medium shadow-glass animate-fade-in ${
            toast.type === "success"
              ? "bg-tm-success/10 border-tm-success/20 text-tm-success"
              : "bg-tm-danger/10 border-tm-danger/20 text-tm-danger"
          }`}
        >
          {toast.message}
        </div>
      )}

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-tm-text">Fleet Manager</h1>
          <p className="text-sm text-tm-muted mt-1">
            {bots.length} bot{bots.length !== 1 ? "s" : ""} registered
          </p>
        </div>
        <button
          onClick={() => fetchFleet(false)}
          className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm text-tm-muted hover:text-tm-text bg-white/[0.03] hover:bg-white/[0.06] border border-tm-border transition-all duration-200"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      <div
        className={`flex items-center gap-3 px-5 py-4 rounded-2xl border backdrop-blur-glass ${
          agent.agent_online
            ? "bg-tm-success/5 border-tm-success/20"
            : "bg-tm-danger/5 border-tm-danger/20"
        }`}
      >
        {agent.agent_online ? (
          <Wifi className="w-5 h-5 text-tm-success flex-shrink-0" />
        ) : (
          <WifiOff className="w-5 h-5 text-tm-danger flex-shrink-0" />
        )}
        <div className="flex-1">
          <p
            className={`text-sm font-medium ${
              agent.agent_online ? "text-tm-success" : "text-tm-danger"
            }`}
          >
            {agent.agent_online
              ? "AlmaLinux Agent Online"
              : "AlmaLinux Agent Offline"}
          </p>
          <p className="text-xs text-tm-muted mt-0.5">
            {agent.agent_online && agent.last_seen_seconds_ago !== undefined
              ? `Last heartbeat ${Math.round(agent.last_seen_seconds_ago)}s ago`
              : "Fleet commands will queue until the agent reconnects"}
          </p>
        </div>
        {!agent.agent_online && (
          <AlertTriangle className="w-5 h-5 text-tm-danger/60 flex-shrink-0" />
        )}
      </div>

      {error && (
        <div className="flex items-center gap-3 px-5 py-4 rounded-2xl border bg-tm-danger/5 border-tm-danger/20">
          <AlertTriangle className="w-5 h-5 text-tm-danger flex-shrink-0" />
          <p className="text-sm text-tm-danger">{error}</p>
        </div>
      )}

      {bots.length === 0 && !error && (
        <div className="flex flex-col items-center justify-center py-16 text-center">
          <Server className="w-12 h-12 text-tm-muted/30 mb-4" />
          <p className="text-tm-muted text-sm">No bots registered in fleet</p>
          <p className="text-tm-muted/60 text-xs mt-1">
            Add bots via the database to get started
          </p>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
        {bots.map((bot) => {
          const style =
            STATUS_STYLES[bot.status] ?? STATUS_STYLES["UNKNOWN"];
          const isRunning = bot.status === "RUNNING";
          const isStopped =
            bot.status === "EXITED" ||
            bot.status === "NOT_RUNNING" ||
            bot.status === "CRASHED";
          const isTransitioning =
            bot.status === "STARTING" || bot.status === "STOPPING";

          return (
            <div
              key={bot.bot_id}
              className={`rounded-2xl border backdrop-blur-glass p-5 transition-all duration-200 hover:shadow-glass ${style.bg}`}
            >
              <div className="flex items-start justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-xl bg-white/[0.04] border border-tm-border flex items-center justify-center">
                    <Server className="w-5 h-5 text-tm-muted" />
                  </div>
                  <div>
                    <p
                      className="text-sm font-mono font-medium text-tm-text"
                      title={bot.bot_id}
                    >
                      {truncateId(bot.bot_id)}
                    </p>
                    {bot.pid && (
                      <p className="text-[11px] text-tm-muted font-mono">
                        PID {bot.pid}
                      </p>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <span
                    className={`w-2 h-2 rounded-full flex-shrink-0 ${style.dot}`}
                  />
                  <span
                    className={`text-xs font-medium ${style.label}`}
                  >
                    {bot.status}
                  </span>
                </div>
              </div>

              <div className="flex items-center gap-4 mb-4 text-xs text-tm-muted">
                <div className="flex items-center gap-1.5">
                  <Clock className="w-3.5 h-3.5" />
                  <span>{formatUptime(bot.uptime_seconds)}</span>
                </div>
                {bot.exit_code !== undefined && bot.exit_code !== null && (
                  <div className="flex items-center gap-1.5">
                    <span>Exit: {bot.exit_code}</span>
                  </div>
                )}
                {bot.memory_mb !== undefined && (
                  <div className="flex items-center gap-1.5">
                    <span>{bot.memory_mb} MB</span>
                  </div>
                )}
              </div>

              <div className="flex items-center gap-2">
                <button
                  onClick={() => handleAction(bot.bot_id, "start")}
                  disabled={
                    !canControl ||
                    isRunning ||
                    isTransitioning ||
                    pendingAction === `${bot.bot_id}-start`
                  }
                  className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium bg-tm-success/10 text-tm-success border border-tm-success/20 hover:bg-tm-success/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all duration-200"
                >
                  {pendingAction === `${bot.bot_id}-start` ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <Play className="w-3.5 h-3.5" />
                  )}
                  Start
                </button>
                <button
                  onClick={() => handleAction(bot.bot_id, "stop")}
                  disabled={
                    !canControl ||
                    isStopped ||
                    isTransitioning ||
                    pendingAction === `${bot.bot_id}-stop`
                  }
                  className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium bg-tm-danger/10 text-tm-danger border border-tm-danger/20 hover:bg-tm-danger/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all duration-200"
                >
                  {pendingAction === `${bot.bot_id}-stop` ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <Square className="w-3.5 h-3.5" />
                  )}
                  Stop
                </button>
                <button
                  onClick={() => handleAction(bot.bot_id, "restart")}
                  disabled={
                    !canControl ||
                    isTransitioning ||
                    pendingAction === `${bot.bot_id}-restart`
                  }
                  className="flex-1 flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg text-xs font-medium bg-tm-accent/10 text-tm-accent border border-tm-accent/20 hover:bg-tm-accent/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all duration-200"
                >
                  {pendingAction === `${bot.bot_id}-restart` ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <RotateCcw className="w-3.5 h-3.5" />
                  )}
                  Restart
                </button>
              </div>

              {!canControl && (
                <p className="text-[10px] text-tm-muted/60 text-center mt-2">
                  Requires admin or owner clearance
                </p>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
