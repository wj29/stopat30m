import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AuthProvider, useAuth } from './contexts/AuthContext';
import Shell from './components/Shell';
import HomePage from './pages/HomePage';
import AnalysisPage from './pages/AnalysisPage';
import TradingPage from './pages/TradingPage';
import SignalPage from './pages/SignalPage';
import BacktestPage from './pages/BacktestPage';
import SettingsPage from './pages/SettingsPage';
import UsersPage from './pages/UsersPage';
import MarketReviewPage from './pages/MarketReviewPage';
import ModelLabPage from './pages/ModelLabPage';
import WatchlistPage from './pages/WatchlistPage';
import ChatPage from './pages/ChatPage';
import RecordManagePage from './pages/RecordManagePage';
import LoginPage from './pages/LoginPage';
import RegisterPage from './pages/RegisterPage';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, staleTime: 30_000 },
  },
});

function RequireAuth() {
  const { token, loading } = useAuth();

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <p className="text-gray-500">loading...</p>
      </div>
    );
  }

  if (!token) {
    return <Navigate to="/login" replace />;
  }

  return <Shell />;
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/register" element={<RegisterPage />} />
            <Route element={<RequireAuth />}>
              <Route path="/" element={<HomePage />} />
              <Route path="/chat" element={<ChatPage />} />
              <Route path="/analysis" element={<AnalysisPage />} />
              <Route path="/watchlist" element={<WatchlistPage />} />
              <Route path="/market-review" element={<MarketReviewPage />} />
              <Route path="/trading" element={<TradingPage />} />
              <Route path="/signals" element={<SignalPage />} />
              <Route path="/backtest" element={<BacktestPage />} />
              <Route path="/model-lab" element={<ModelLabPage />} />
              <Route path="/users" element={<UsersPage />} />
              <Route path="/records" element={<RecordManagePage />} />
              <Route path="/settings" element={<SettingsPage />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </AuthProvider>
    </QueryClientProvider>
  );
}
