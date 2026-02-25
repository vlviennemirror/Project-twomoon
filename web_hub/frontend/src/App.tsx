import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider, useAuth } from "@/context/AuthContext";
import DashboardLayout from "@/components/DashboardLayout";
import DashboardPage from "@/pages/DashboardPage";
import FleetPage from "@/pages/FleetPage";
import SettingsPage from "@/pages/SettingsPage";
import LeaderboardPage from "@/pages/LeaderboardPage";
import ModerationPage from "@/pages/ModerationPage";
import { Loader2 } from "lucide-react";

function AuthGate() {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return (
      <div className="min-h-screen bg-tm-bg flex items-center justify-center">
        <div className="flex flex-col items-center gap-4 animate-fade-in">
          <Loader2 className="w-8 h-8 text-tm-accent animate-spin" />
          <p className="text-tm-muted text-sm">Verifying session...</p>
        </div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  return <DashboardLayout />;
}

function LoginPage() {
  const { isAuthenticated, isLoading } = useAuth();

  if (isLoading) {
    return (
      <div className="min-h-screen bg-tm-bg flex items-center justify-center">
        <Loader2 className="w-8 h-8 text-tm-accent animate-spin" />
      </div>
    );
  }

  if (isAuthenticated) {
    return <Navigate to="/dashboard" replace />;
  }

  return (
    <div className="min-h-screen bg-tm-bg flex items-center justify-center px-4">
      <div className="bg-glass-gradient backdrop-blur-glass border border-tm-border shadow-glass rounded-2xl p-10 max-w-sm w-full text-center animate-fade-in">
        <h1 className="text-3xl font-bold text-tm-text mb-2">Two Moon</h1>
        <p className="text-tm-muted text-sm mb-8">Ecosystem Control Center</p>
        <a
          href="/auth/login"
          className="inline-flex items-center justify-center gap-2 w-full px-6 py-3 bg-tm-accent hover:bg-tm-accent-dim text-white font-medium rounded-xl transition-colors duration-200"
        >
          Sign in with Discord
        </a>
        <p className="text-tm-muted text-xs mt-6">Authorized personnel only</p>
      </div>
    </div>
  );
}

function NotFoundPage() {
  return (
    <div className="min-h-screen bg-tm-bg flex items-center justify-center">
      <div className="text-center animate-fade-in">
        <h1 className="text-6xl font-bold text-tm-accent mb-4">404</h1>
        <p className="text-tm-muted">This sector doesn't exist.</p>
        <a
          href="/dashboard"
          className="inline-block mt-6 text-tm-accent hover:text-tm-accent-dim transition-colors"
        >
          Return to Dashboard
        </a>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          <Route path="/login" element={<LoginPage />} />

          <Route element={<AuthGate />}>
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route path="/fleet" element={<FleetPage />} />
            <Route path="/settings" element={<SettingsPage />} />
            <Route path="/leaderboard" element={<LeaderboardPage />} />
            <Route path="/moderation" element={<ModerationPage />} />
          </Route>

          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="*" element={<NotFoundPage />} />
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  );
}
