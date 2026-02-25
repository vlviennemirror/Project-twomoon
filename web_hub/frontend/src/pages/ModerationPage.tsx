import { useState, useEffect, useCallback, useRef } from "react";
import { useAuth } from "@/context/AuthContext";
import GlassCard from "@/components/GlassCard";
import api from "@/lib/api";
import {
  Shield,
  Brain,
  AlertTriangle,
  Ban,
  Clock,
  MessageSquareOff,
  Loader2,
  RefreshCw,
  Activity,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";

interface ModerationMetrics {
  total_caught: number;
  total_regex: number;
  total_ai: number;
  avg_confidence: number;
  circuit_breaker_status: string;
  strikes_today: number;
}

interface StrikeEntry {
  id: string;
  user_id: string;
  username: string;
  tier: string;
  reason: string;
  confidence: number | null;
  source: string;
  created_at: string;
}

interface StrikesResponse {
  strikes: StrikeEntry[];
  total: number;
  page: number;
  page_size: number;
}

const TIER_STYLES: Record<string, { icon: React.ComponentType<{ className?: string }>; color: string; bg: string }> = {
  WARN: {
    icon: AlertTriangle,
    color: "text-tm-warning",
    bg: "bg-tm-warning/10 border-tm-warning/20",
  },
  MUTE: {
    icon: MessageSquareOff,
    color: "text-orange-400",
    bg: "bg-orange-400/10 border-orange-400/20",
  },
  KICK: {
    icon: AlertTriangle,
    color: "text-tm-danger",
    bg: "bg-tm-danger/10 border-tm-danger/20",
  },
  BAN: {
    icon: Ban,
    color: "text-red-500",
    bg: "bg-red-500/10 border-red-500/20",
  },
};

function formatTimestamp(iso: string): string {
  const d = new Date(iso);
  const now = new Date();
  const diffMs = now.getTime() - d.getTime();
  const diffMin = Math.floor(diffMs / 60000);

  if (diffMin < 1) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDay = Math.floor(diffHr / 24);
  if (diffDay < 7) return `${diffDay}d ago`;

  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export default function ModerationPage() {
  const { hasClearance } = useAuth();

  const [metrics, setMetrics] = useState<ModerationMetrics>({
    total_caught: 0,
    total_regex: 0,
    total_ai: 0,
    avg_confidence: 0,
    circuit_breaker_status: "UNKNOWN",
    strikes_today: 0,
  });
  const [strikes, setStrikes] = useState<StrikeEntry[]>([]);
  const [totalStrikes, setTotalStrikes] = useState(0);
  const [page, setPage] = useState(1);
  const [isLoadingMetrics, setIsLoadingMetrics] = useState(true);
  const [isLoadingStrikes, setIsLoadingStrikes] = useState(true);

  const pageSize = 15;

  const metricsAbortRef = useRef<AbortController | null>(null);
  const strikesAbortRef = useRef<AbortController | null>(null);
  const isMountedRef = useRef(true);

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      metricsAbortRef.current?.abort();
      strikesAbortRef.current?.abort();
    };
  }, []);

  const fetchMetrics = useCallback(async () => {
    metricsAbortRef.current?.abort();
    const controller = new AbortController();
    metricsAbortRef.current = controller;

    try {
      const res = await api.get<ModerationMetrics>("/api/moderation/metrics", {
        signal: controller.signal,
      });
      if (!isMountedRef.current) return;
      setMetrics(res.data);
    } catch (err: unknown) {
      if (err instanceof Error && (err as { name: string }).name === "CanceledError") return;
    } finally {
      if (isMountedRef.current) setIsLoadingMetrics(false);
    }
  }, []);

  const fetchStrikes = useCallback(async (p: number) => {
    strikesAbortRef.current?.abort();
    const controller = new AbortController();
    strikesAbortRef.current = controller;

    setIsLoadingStrikes(true);
    try {
      const res = await api.get<StrikesResponse>("/api/moderation/strikes", {
        params: { page: p, page_size: pageSize },
        signal: controller.signal,
      });
      if (!isMountedRef.current) return;
      setStrikes(res.data.strikes);
      setTotalStrikes(res.data.total);
    } catch (err: unknown) {
      if (err instanceof Error && (err as { name: string }).name === "CanceledError") return;
      if (isMountedRef.current) {
        setStrikes([]);
        setTotalStrikes(0);
      }
    } finally {
      if (isMountedRef.current) setIsLoadingStrikes(false);
    }
  }, []);

  useEffect(() => {
    fetchMetrics();
  }, [fetchMetrics]);

  useEffect(() => {
    fetchStrikes(page);
  }, [page, fetchStrikes]);

  const totalPages = Math.max(1, Math.ceil(totalStrikes / pageSize));

  const cbColor =
    metrics.circuit_breaker_status === "CLOSED"
      ? "text-tm-success"
      : metrics.circuit_breaker_status === "OPEN"
        ? "text-tm-danger"
        : "text-tm-warning";

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-tm-text">Moderation</h1>
          <p className="text-sm text-tm-muted mt-1">
            Apostle AI moderation pipeline overview
          </p>
        </div>
        <button
          onClick={() => {
            fetchMetrics();
            fetchStrikes(page);
          }}
          className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm text-tm-muted hover:text-tm-text bg-white/[0.03] hover:bg-white/[0.06] border border-tm-border transition-all duration-200"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {isLoadingMetrics ? (
        <div className="flex items-center justify-center h-32">
          <Loader2 className="w-6 h-6 text-tm-accent animate-spin" />
        </div>
      ) : (
        <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
          <GlassCard>
            <p className="text-[10px] text-tm-muted uppercase tracking-wider">
              Total Caught
            </p>
            <p className="text-2xl font-bold text-tm-danger mt-1">
              {metrics.total_caught.toLocaleString()}
            </p>
          </GlassCard>
          <GlassCard>
            <p className="text-[10px] text-tm-muted uppercase tracking-wider">
              Regex Catches
            </p>
            <p className="text-2xl font-bold text-tm-warning mt-1">
              {metrics.total_regex.toLocaleString()}
            </p>
          </GlassCard>
          <GlassCard>
            <p className="text-[10px] text-tm-muted uppercase tracking-wider">
              AI Catches
            </p>
            <p className="text-2xl font-bold text-tm-accent mt-1">
              {metrics.total_ai.toLocaleString()}
            </p>
          </GlassCard>
          <GlassCard>
            <p className="text-[10px] text-tm-muted uppercase tracking-wider">
              Avg Confidence
            </p>
            <p className="text-2xl font-bold text-tm-accent mt-1">
              {metrics.avg_confidence > 0
                ? `${(metrics.avg_confidence * 100).toFixed(0)}%`
                : "—"}
            </p>
          </GlassCard>
          <GlassCard>
            <p className="text-[10px] text-tm-muted uppercase tracking-wider">
              Circuit Breaker
            </p>
            <p className={`text-2xl font-bold mt-1 ${cbColor}`}>
              {metrics.circuit_breaker_status}
            </p>
          </GlassCard>
          <GlassCard>
            <p className="text-[10px] text-tm-muted uppercase tracking-wider">
              Today
            </p>
            <p className="text-2xl font-bold text-tm-danger mt-1">
              {metrics.strikes_today}
            </p>
          </GlassCard>
        </div>
      )}

      <GlassCard padding={false}>
        <div className="flex items-center justify-between px-6 py-4 border-b border-tm-border">
          <div className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-tm-danger" />
            <h3 className="text-base font-semibold text-tm-text">
              Strike Log
            </h3>
          </div>
          <span className="text-xs text-tm-muted">
            {totalStrikes.toLocaleString()} total records
          </span>
        </div>

        {isLoadingStrikes ? (
          <div className="flex items-center justify-center h-64">
            <Loader2 className="w-6 h-6 text-tm-accent animate-spin" />
          </div>
        ) : strikes.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-64">
            <Activity className="w-10 h-10 text-tm-muted/30 mb-3" />
            <p className="text-sm text-tm-muted">No strikes recorded</p>
          </div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-tm-border text-[11px] text-tm-muted uppercase tracking-wider">
                    <th className="text-left py-3 px-6 w-40">User</th>
                    <th className="text-center py-3 px-4 w-24">Tier</th>
                    <th className="text-center py-3 px-4 w-24">Source</th>
                    <th className="text-left py-3 px-4">Reason</th>
                    <th className="text-center py-3 px-4 w-24">Conf.</th>
                    <th className="text-right py-3 px-6 w-32">Time</th>
                  </tr>
                </thead>
                <tbody>
                  {strikes.map((strike) => {
                    const tierStyle = TIER_STYLES[strike.tier] ?? TIER_STYLES["WARN"];
                    const TierIcon = tierStyle.icon;
                    return (
                      <tr
                        key={strike.id}
                        className="border-b border-tm-border/50 hover:bg-white/[0.02] transition-colors duration-150"
                      >
                        <td className="py-3 px-6">
                          <div className="flex items-center gap-2">
                            <div className="w-6 h-6 rounded-full bg-white/[0.05] flex items-center justify-center text-[10px] font-bold text-tm-muted">
                              {strike.username.charAt(0).toUpperCase()}
                            </div>
                            <span className="text-sm text-tm-text truncate max-w-[120px]">
                              {strike.username}
                            </span>
                          </div>
                        </td>
                        <td className="py-3 px-4 text-center">
                          <span
                            className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-medium border ${tierStyle.bg}`}
                          >
                            <TierIcon className="w-3 h-3" />
                            {strike.tier}
                          </span>
                        </td>
                        <td className="py-3 px-4 text-center">
                          <span
                            className={`text-xs font-medium ${
                              strike.source === "AI"
                                ? "text-tm-accent"
                                : "text-tm-warning"
                            }`}
                          >
                            {strike.source}
                          </span>
                        </td>
                        <td className="py-3 px-4">
                          <p className="text-sm text-tm-muted truncate max-w-xs" title={strike.reason}>
                            {strike.reason}
                          </p>
                        </td>
                        <td className="py-3 px-4 text-center">
                          {strike.confidence !== null ? (
                            <span
                              className={`text-xs font-mono ${
                                strike.confidence >= 0.9
                                  ? "text-tm-success"
                                  : strike.confidence >= 0.75
                                    ? "text-tm-warning"
                                    : "text-tm-muted"
                              }`}
                            >
                              {(strike.confidence * 100).toFixed(0)}%
                            </span>
                          ) : (
                            <span className="text-xs text-tm-muted">—</span>
                          )}
                        </td>
                        <td className="py-3 px-6 text-right">
                          <span className="text-xs text-tm-muted">
                            {formatTimestamp(strike.created_at)}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between px-6 py-3 border-t border-tm-border">
              <p className="text-xs text-tm-muted">
                Page {page} of {totalPages}
              </p>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1}
                  className="p-1.5 rounded-lg text-tm-muted hover:text-tm-text hover:bg-white/[0.05] disabled:opacity-30 disabled:cursor-not-allowed transition-all"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages}
                  className="p-1.5 rounded-lg text-tm-muted hover:text-tm-text hover:bg-white/[0.05] disabled:opacity-30 disabled:cursor-not-allowed transition-all"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            </div>
          </>
        )}
      </GlassCard>
    </div>
  );
}
