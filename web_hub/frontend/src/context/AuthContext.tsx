import {
  createContext,
  useContext,
  useState,
  useEffect,
  useMemo,
  useCallback,
  type ReactNode,
} from "react";
import axios from "axios";
import api from "@/lib/api";

interface User {
  user_id: string;
  username: string;
  clearance: string;
  roles: string[];
  token_issued_at: number;
  token_expires_at: number;
}

interface AuthState {
  user: User | null;
  isLoading: boolean;
  isAuthenticated: boolean;
  error: string | null;
  logout: () => Promise<void>;
  refresh: () => Promise<void>;
  hasClearance: (...levels: string[]) => boolean;
}

const AuthContext = createContext<AuthState | undefined>(undefined);

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const hydrate = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);

      const response = await api.get<User>("/auth/me");
      setUser(response.data);
    } catch (err: unknown) {
      setUser(null);

      if (axios.isAxiosError(err)) {
        const status = err.response?.status;

        if (!status) {
          setError("Network error");
        } else if (status !== 401) {
          setError("Failed to verify session");
        }
      } else {
        setError("Unexpected error occurred");
      }
    } finally {
      setIsLoading(false);
    }
  }, []);

  const logout = useCallback(async () => {
    try {
      await api.post("/auth/logout");
    } catch {
    } finally {
      setUser(null);
      setError(null);
      window.location.href = "/login";
    }
  }, []);

  const hasClearance = useCallback(
    (...levels: string[]) => {
      if (!user) return false;
      return levels.includes(user.clearance);
    },
    [user]
  );

  useEffect(() => {
    hydrate();
  }, [hydrate]);

  const value = useMemo<AuthState>(
    () => ({
      user,
      isLoading,
      isAuthenticated: user !== null,
      error,
      logout,
      refresh: hydrate,
      hasClearance,
    }),
    [user, isLoading, error, logout, hydrate, hasClearance]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthState {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}