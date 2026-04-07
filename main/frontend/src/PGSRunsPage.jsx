import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAppState, useAppDispatch } from './context.jsx';
import PGSRunsPanel from './components/PGSRunsPanel.jsx';
import StatusBar from './components/StatusBar.jsx';
import Login from './components/Login.jsx';
import useSystemStats from './hooks/useSystemStats.js';
import './App.css';

export default function PGSRunsPage() {
  const [user, setUser] = useState(() => {
    const token = localStorage.getItem('auth_token');
    return token ? { token } : null;
  });
  const [authChecked, setAuthChecked] = useState(false);
  const navigate = useNavigate();
  const { stats: sysStats, loading: sysLoading } = useSystemStats(5000);

  React.useEffect(() => {
    const token = localStorage.getItem('auth_token');
    if (!token) { setAuthChecked(true); return; }
    const base = '/';
    fetch(base + 'api/auth/me', { headers: { Authorization: 'Bearer ' + token } })
      .then(r => { if (!r.ok) throw new Error(); return r.json(); })
      .then(data => { setUser({ token, ...data }); setAuthChecked(true); })
      .catch(() => { localStorage.removeItem('auth_token'); setUser(null); setAuthChecked(true); });
  }, []);

  const handleLogout = () => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('refresh_token');
    setUser(null);
  };

  if (!authChecked) return null;
  if (!user) return <Login onLogin={(data) => setUser({ token: data.access_token, ...data.user })} />;

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-logo" style={{ cursor: 'pointer' }} onClick={() => navigate('/')}>
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z"/>
            <path d="M8 12s1-4 4-4 4 4 4 4-1 4-4 4-4-4-4-4z"/>
          </svg>
          <span className="app-title">23andClaude</span>
        </div>
        <nav className="tab-nav">
          <button className="tab-btn" onClick={() => navigate('/')}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M15 18l-6-6 6-6" />
            </svg>
            <span className="tab-label">Back to Dashboard</span>
          </button>
          <button className="tab-btn active">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4" />
            </svg>
            <span className="tab-label">PGS Runs</span>
          </button>
        </nav>
        <button onClick={handleLogout} style={{
          background: 'transparent', border: '1px solid #30363d', color: '#8b949e',
          padding: '4px 12px', borderRadius: 6, cursor: 'pointer', fontSize: 12,
          marginLeft: 'auto', marginRight: 12,
        }}>Logout</button>
      </header>
      <StatusBar stats={sysStats} loading={sysLoading} />
      <main className="app-main">
        <PGSRunsPanel />
      </main>
    </div>
  );
}
