import { useState, useEffect, useCallback } from "react";
import GlassCard from "@/components/GlassCard";
import api from "@/lib/api";
import {
  Trophy,
  Crown,
  Medal,
  Star,
  Users,
  Loader2,
  ChevronUp,
} from "lucide-react";

interface LeaderboardEntry {
  rank: number;
  user_id: string;
  username: string;
  level: number;
  xp: number;
  total_messages: number;
}

interface LeaderboardResponse {
  faction: string;
  entries: LeaderboardEntry[];
  total_members: number;
}

type Faction = "M" | "F";

const FACTION_META: Record<Faction, { label: string; color: string; activeBg: string }> = {
  M: {
    label: "Member Path",
    color: "text-tm-accent",
    activeBg: "bg-tm-accent/10 border-tm-accent/30 text-tm-accent",
  },
  F: {
    label: "Friends Path",
    color: "text-tm-success",
    activeBg: "bg-tm-success/10 border-tm-success/30 text-tm-success",
  },
};

function RankBadge({ rank }: { rank: number }) {
  if (rank === 1) {
    return <Crown className="w-5 h-5 text-yellow-400" />;
  }
  if (rank === 2) {
    return <Medal className="w-5 h-5 text-gray-300" />;
  }
  if (rank === 3) {
    return <Medal className="w-5 h-5 text-amber-600" />;
  }
  return (
    <span className="w-5 h-5 flex items-center justify-center text-xs font-bold text-tm-muted">
      {rank}
    </span>
  );
}

function formatXp(xp: number): string {
  if (xp >= 1_000_000) return `${(xp / 1_000_000).toFixed(1)}M`;
  if (xp >= 1_000) return `${(xp / 1_000).toFixed(1)}K`;
  return xp.toLocaleString();
}

export default function LeaderboardPage() {
  const [faction, setFaction] = useState<Faction>("M");
  const [data, setData] = useState<LeaderboardResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  const fetchLeaderboard = useCallback(async (f: Faction) => {
    setIsLoading(true);
    try {
      const res = await api.get<LeaderboardResponse>(
        `/api/leaderboard/${f}`,
        { params: { limit: 10 } },
      );
      setData(res.data);
    } catch {
      setData({ faction: f, entries: [], total_members: 0 });
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchLeaderboard(faction);
  }, [faction, fetchLeaderboard]);

  const meta = FACTION_META[faction];
  const entries = data?.entries ?? [];

  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-tm-text">Leaderboard</h1>
          <p className="text-sm text-tm-muted mt-1">
            Top players by XP across both factions
          </p>
        </div>
        <div className="flex items-center gap-1 p-1 rounded-xl bg-white/[0.03] border border-tm-border">
          {(["M", "F"] as Faction[]).map((f) => (
            <button
              key={f}
              onClick={() => setFaction(f)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 border ${
                faction === f
                  ? FACTION_META[f].activeBg
                  : "border-transparent text-tm-muted hover:text-tm-text"
              }`}
            >
              {FACTION_META[f].label}
            </button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <GlassCard>
          <div className="flex items-center gap-3">
            <Users className={`w-5 h-5 ${meta.color}`} />
            <div>
              <p className="text-xs text-tm-muted uppercase tracking-wider">
                Total Members
              </p>
              <p className={`text-2xl font-bold ${meta.color}`}>
                {data?.total_members?.toLocaleString() ?? "—"}
              </p>
            </div>
          </div>
        </GlassCard>
        <GlassCard>
          <div className="flex items-center gap-3">
            <Trophy className={`w-5 h-5 ${meta.color}`} />
            <div>
              <p className="text-xs text-tm-muted uppercase tracking-wider">
                #1 Level
              </p>
              <p className={`text-2xl font-bold ${meta.color}`}>
                {entries.length > 0 ? entries[0].level : "—"}
              </p>
            </div>
          </div>
        </GlassCard>
      </div>

      <GlassCard padding={false}>
        {isLoading ? (
          <div className="flex items-center justify-center h-64">
            <Loader2 className="w-6 h-6 text-tm-accent animate-spin" />
          </div>
        ) : entries.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-64 text-center">
            <Star className="w-10 h-10 text-tm-muted/30 mb-3" />
            <p className="text-sm text-tm-muted">
              No rankings yet for {meta.label}
            </p>
          </div>
        ) : (
          <table className="w-full">
            <thead>
              <tr className="border-b border-tm-border text-xs text-tm-muted uppercase tracking-wider">
                <th className="text-left py-4 px-6 w-16">Rank</th>
                <th className="text-left py-4 px-4">Player</th>
                <th className="text-center py-4 px-4 w-24">Level</th>
                <th className="text-right py-4 px-6 w-32">XP</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((entry, idx) => (
                <tr
                  key={entry.user_id}
                  className={`border-b border-tm-border/50 transition-colors duration-150 hover:bg-white/[0.02] ${
                    idx === 0 ? "bg-white/[0.02]" : ""
                  }`}
                >
                  <td className="py-4 px-6">
                    <RankBadge rank={entry.rank} />
                  </td>
                  <td className="py-4 px-4">
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 rounded-full bg-tm-accent/10 flex items-center justify-center text-xs font-bold text-tm-accent">
                        {entry.username.charAt(0).toUpperCase()}
                      </div>
                      <div>
                        <p className="text-sm font-medium text-tm-text">
                          {entry.username}
                        </p>
                        <p className="text-[11px] text-tm-muted">
                          {entry.total_messages.toLocaleString()} messages
                        </p>
                      </div>
                    </div>
                  </td>
                  <td className="py-4 px-4 text-center">
                    <div className="inline-flex items-center gap-1">
                      <ChevronUp className={`w-3.5 h-3.5 ${meta.color}`} />
                      <span className={`text-sm font-bold ${meta.color}`}>
                        {entry.level}
                      </span>
                    </div>
                  </td>
                  <td className="py-4 px-6 text-right">
                    <span className="text-sm font-mono text-tm-text">
                      {formatXp(entry.xp)}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </GlassCard>
    </div>
  );
}
