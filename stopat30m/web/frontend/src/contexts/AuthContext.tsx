import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { getMe, getAuthSettings, login as apiLogin, register as apiRegister, type UserInfo, type AuthSettings } from '../api/auth';

interface AuthState {
  token: string | null;
  user: UserInfo | null;
  loading: boolean;
  login: (username: string, password: string) => Promise<void>;
  register: (username: string, password: string, inviteCode: string) => Promise<void>;
  logout: () => void;
  isAdmin: boolean;
  settings: AuthSettings;
}

const AuthContext = createContext<AuthState | undefined>(undefined);

const TOKEN_KEY = 'access_token';

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem(TOKEN_KEY));
  const [user, setUser] = useState<UserInfo | null>(null);
  const [resolvedToken, setResolvedToken] = useState<string | null>(null);
  const loading = token !== null && resolvedToken !== token;
  const [settings, setSettings] = useState<AuthSettings>({ single_user: true, registration_enabled: false });

  useEffect(() => { getAuthSettings().then(setSettings).catch(() => { /* Fail closed for registration. */ }); }, []);

  useEffect(() => {
    if (!token) return;
    let active = true;
    getMe()
      .then(value => { if (active) setUser(value); })
      .catch(() => {
        if (!active) return;
        localStorage.removeItem(TOKEN_KEY);
        setToken(null);
        setUser(null);
      })
      .finally(() => { if (active) setResolvedToken(token); });
    return () => { active = false; };
  }, [token]);

  const login = useCallback(async (username: string, password: string) => {
    const res = await apiLogin(username, password);
    localStorage.setItem(TOKEN_KEY, res.access_token);
    setResolvedToken(null);
    setToken(res.access_token);
  }, []);

  const register = useCallback(async (username: string, password: string, inviteCode: string) => {
    const res = await apiRegister(username, password, inviteCode);
    localStorage.setItem(TOKEN_KEY, res.access_token);
    setResolvedToken(null);
    setToken(res.access_token);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem(TOKEN_KEY);
    setToken(null);
    setUser(null);
    setResolvedToken(null);
  }, []);

  const isAdmin = user?.role === 'admin';

  const value = useMemo(
    () => ({ token, user, loading, login, register, logout, isAdmin, settings }),
    [token, user, loading, login, register, logout, isAdmin, settings],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// Context hooks are intentionally exported alongside their provider.
// eslint-disable-next-line react-refresh/only-export-components
export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
