import { useState, useEffect } from "react";
import { useAuth } from "@/context/AuthContext";
import GlassCard from "@/components/GlassCard";
import api from "@/lib/api";
import {
  Server,
  Users,
  Shield,
  Zap,
  HandshakeIcon,
  Brain,
  TrendingUp,
  Activity,
  Loader2,
} from "lucide-react";

interface FleetSummary {
  total: number;
  alive: number;
}

interface AgentSummary {
  agent_online: boolean;
}

interface Metrics {
  fleet: FleetSummary;
  agent: AgentSummary;
  total_users: number;
  total_vouched: number;
  total_strikes: number;
  total_xp_processed: number;
  avg_ai_confidence: number;
  active_today: number;
}

const EMPTY_METRICS: Metrics = {
  fleet: { total: 0, alive: 0 },
  agent: { agent_online: false },
  total_users: 0,
  total_vouched: 0,
  total_strikes: 0,
  total_xp_processed: 0,
  avg_ai_confidence: 0,
  active_today: 0,
};

interface MetricCardProps {
  label: string;
  value: string | number;
  subtitle?: string;
  icon: React.ComponentType<{ className?: string }>;
  accent?: string;
}

function MetricCard({ label, value, subtitle, icon: Icon, accent = "text-tm-accent" }: MetricCardProps) {
  return (
    <GlassCard>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs text-tm-muted font-medium uppercase tracking-wider">
            {label}
          </p>
          <p className={`text-3xl font-bold mt-2 ${accent}`}>{value}</p>
          {subtitle && (
            <p className="text-xs text-tm-muted mt-1">{subtitle}</p>
          )}
        </div>
        <div className="w-10 h-10 rounded-xl bg-white/[0.04] border border-tm-border flex items-center justify-center">
          <Icon className={`w-5 h-5 ${accent}`} />
        </div>
      </div>
    </GlassCard>
  );
}

export default function DashboardPage() {
  const { user } = useAuth();
  const [metrics, setMetrics] = useState<Metrics>(EMPTY_METRICS);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    (async () => {
      try {
        const healthRes = await api.get<{
          fleet: FleetSummary;
          services: { agent: AgentSummary };
        }>("/health");

        let totalUsers = 0;
        let totalVouched = 0;
        let totalStrikes = 0;
        let totalXp = 0;
        let avgConfidence = 0;
        let activeToday = 0;

        try {
          const statsRes = await api.get<{
            total_users: number;
            total_vouched: number;
            total_strikes: number;
            total_xp_processed: number;
            avg_ai_confidence: number;
            active_today: number;
          }>("/api/stats/overview");
          totalUsers = statsRes.data.total_users;
          totalVouched = statsRes.data.total_vouched;
          totalStrikes = statsRes.data.total_strikes;
          totalXp = statsRes.data.total_xp_processed;
          avgConfidence = statsRes.data.avg_ai_confidence;
          activeToday = statsRes.data.active_today;
        } catch {
          totalUsers = 0;
          totalVouched = 0;
          totalStrikes = 0;
          totalXp = 0;
          avgConfidence = 0;
          activeToday = 0;
        }

        setMetrics({
          fleet: healthRes.data.fleet,
          agent: healthRes.data.services?.agent ?? { agent_online: false },
          total_users: totalUsers,
          total_vouched: totalVouched,
          total_strikes: totalStrikes,
          total_xp_processed: totalXp,
          avg_ai_confidence: avgConfidence,
          active_today: activeToday,
        });
      } catch {
        setMetrics(EMPTY_METRICS);
      } finally {
        setIsLoading(false);
      }
    })();
  }, []);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 text-tm-accent animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-8 animate-fade-in">
      <div>
        <h1 className="text-2xl font-bold text-tm-text">
          Welcome back, {user?.username}
        </h1>
        <p className="text-sm text-tm-muted mt-1">
          Two Moon Ecosystem at a glance
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
        <MetricCard
          label="Fleet Status"
          value={`${metrics.fleet.alive}/${metrics.fleet.total}`}
          subtitle={
            metrics.agent.agent_online
              ? "Agent online"
              : "Agent offline"
          }
          icon={Server}
          accent={
            metrics.fleet.alive === metrics.fleet.total && metrics.fleet.total > 0
              ? "text-tm-success"
              : "text-tm-warning"
          }
        />
        <MetricCard
          label="Active Today"
          value={metrics.active_today}
          subtitle="Unique users"
          icon={Activity}
          accent="text-tm-accent"
        />
        <MetricCard
          label="Total Users"
          value={metrics.total_users.toLocaleString()}
          subtitle="Across all factions"
          icon={Users}
          accent="text-tm-accent"
        />
        <MetricCard
          label="Vouched Members"
          value={metrics.total_vouched.toLocaleString()}
          subtitle="Member path recruits"
          icon={HandshakeIcon}
          accent="text-tm-success"
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
        <MetricCard
          label="XP Processed"
          value={
            metrics.total_xp_processed >= 1_000_000
              ? `${(metrics.total_xp_processed / 1_000_000).toFixed(1)}M`
              : metrics.total_xp_processed >= 1_000
                ? `${(metrics.total_xp_processed / 1_000).toFixed(1)}K`
                : metrics.total_xp_processed
          }
          subtitle="Lifetime total"
          icon={TrendingUp}
          accent="text-tm-accent"
        />
        <MetricCard
          label="AI Confidence"
          value={
            metrics.avg_ai_confidence > 0
              ? `${(metrics.avg_ai_confidence * 100).toFixed(0)}%`
              : "—"
          }
          subtitle="Average moderation confidence"
          icon={Brain}
          accent="text-tm-accent"
        />
        <MetricCard
          label="Strikes Issued"
          value={metrics.total_strikes.toLocaleString()}
          subtitle="Total moderation actions"
          icon={Shield}
          accent="text-tm-danger"
        />
        <MetricCard
          label="Leveling Engine"
          value={metrics.fleet.alive > 0 ? "Active" : "Offline"}
          subtitle="Write-Behind Cache"
          icon={Zap}
          accent={metrics.fleet.alive > 0 ? "text-tm-success" : "text-tm-danger"}
        />
      </div>
    </div>
  );
}
