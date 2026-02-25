import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";
import {
  LayoutDashboard,
  Server,
  Settings,
  LogOut,
  Shield,
  Moon,
  Trophy,
  ShieldAlert,
} from "lucide-react";

const NAV_ITEMS = [
  { to: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { to: "/fleet", label: "Fleet", icon: Server },
  { to: "/leaderboard", label: "Leaderboard", icon: Trophy },
  { to: "/moderation", label: "Moderation", icon: ShieldAlert },
  { to: "/settings", label: "Settings", icon: Settings },
];

const CLEARANCE_STYLES: Record<string, string> = {
  owner: "bg-tm-accent/20 text-tm-accent border-tm-accent/30",
  admin: "bg-tm-success/20 text-tm-success border-tm-success/30",
  moderator: "bg-tm-warning/20 text-tm-warning border-tm-warning/30",
};

function SidebarLink({
  to,
  label,
  icon: Icon,
}: {
  to: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
}) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `flex items-center gap-3 px-4 py-2.5 rounded-xl text-sm font-medium transition-all duration-200 ${
          isActive
            ? "bg-tm-accent/10 text-tm-accent shadow-glass-inset"
            : "text-tm-muted hover:text-tm-text hover:bg-white/[0.03]"
        }`
      }
    >
      <Icon className="w-[18px] h-[18px] flex-shrink-0" />
      <span>{label}</span>
    </NavLink>
  );
}

export default function DashboardLayout() {
  const { user, logout } = useAuth();
  const location = useLocation();

  const pageTitle =
    NAV_ITEMS.find((item) => location.pathname.startsWith(item.to))?.label ??
    "Dashboard";

  const clearance = user?.clearance ?? "unknown";
  const badgeStyle =
    CLEARANCE_STYLES[clearance] ?? "bg-white/5 text-tm-muted border-white/10";

  return (
    <div className="min-h-screen bg-tm-bg flex">
      <aside className="w-64 flex-shrink-0 border-r border-tm-border bg-tm-surface/50 backdrop-blur-glass flex flex-col">
        <div className="p-6 border-b border-tm-border">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-tm-accent/10 flex items-center justify-center">
              <Moon className="w-5 h-5 text-tm-accent" />
            </div>
            <div>
              <h1 className="text-sm font-bold text-tm-text tracking-wide">
                Two Moon
              </h1>
              <p className="text-[11px] text-tm-muted">Control Center</p>
            </div>
          </div>
        </div>

        <nav className="flex-1 p-4 flex flex-col gap-1">
          {NAV_ITEMS.map((item) => (
            <SidebarLink key={item.to} {...item} />
          ))}
        </nav>

        <div className="p-4 border-t border-tm-border">
          <div className="flex items-center gap-3 px-3 mb-4">
            <div className="w-8 h-8 rounded-full bg-tm-accent/20 flex items-center justify-center text-xs font-bold text-tm-accent">
              {user?.username?.charAt(0).toUpperCase() ?? "?"}
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-sm text-tm-text truncate">
                {user?.username ?? "Unknown"}
              </p>
              <span
                className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium border ${badgeStyle}`}
              >
                <Shield className="w-2.5 h-2.5" />
                {clearance}
              </span>
            </div>
          </div>
          <button
            onClick={logout}
            className="flex items-center gap-3 px-4 py-2.5 rounded-xl text-sm text-tm-muted hover:text-tm-danger hover:bg-tm-danger/5 transition-all duration-200 w-full"
          >
            <LogOut className="w-[18px] h-[18px]" />
            <span>Logout</span>
          </button>
        </div>
      </aside>

      <div className="flex-1 flex flex-col min-h-screen overflow-hidden">
        <header className="h-16 border-b border-tm-border bg-tm-surface/30 backdrop-blur-glass flex items-center justify-between px-8 flex-shrink-0">
          <h2 className="text-lg font-semibold text-tm-text">{pageTitle}</h2>
          <div className="flex items-center gap-3 text-sm text-tm-muted">
            <span>{user?.username}</span>
            <span className="w-1 h-1 rounded-full bg-tm-border" />
            <span
              className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs font-medium border ${badgeStyle}`}
            >
              {clearance}
            </span>
          </div>
        </header>

        <main className="flex-1 overflow-y-auto p-8">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
