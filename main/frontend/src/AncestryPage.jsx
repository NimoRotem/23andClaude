import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import AncestryPanel from './components/AncestryPanel.jsx';
import StatusBar from './components/StatusBar.jsx';
import useSystemStats from './hooks/useSystemStats.js';
import Login from './components/Login';

export default function AncestryPage() {
  const navigate = useNavigate();
  const { stats: sysStats, loading: sysLoading } = useSystemStats(5000);

  const [user, setUser] = useState(() => {
    const token = localStorage.getItem('auth_token');
    return token ? { token } : null;
  });
  const [authChecked, setAuthChecked] = useState(false);

  useEffect(() => {
    const token = localStorage.getItem('auth_token');
    if (!token) { setAuthChecked(true); return; }
    fetch('/api/auth/me', { headers: { Authorization: 'Bearer ' + token } })
      .then(r => { if (!r.ok) throw new Error(); return r.json(); })
      .then(data => { setUser({ token, ...data }); setAuthChecked(true); })
      .catch(() => { localStorage.removeItem('auth_token'); setUser(null); setAuthChecked(true); });
  }, []);

  if (!authChecked) return null;
  if (!user) return <Login onLogin={(data) => setUser({ token: data.access_token, ...data.user })} />;

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-logo">
          <button className="ancestry-back-btn" onClick={() => navigate('/')}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M19 12H5M12 19l-7-7 7-7" />
            </svg>
            Back
          </button>
        </div>
        <span className="app-title" style={{ marginLeft: 12 }}>Ancestry Analysis</span>
        <div style={{ flex: 1 }} />
      </header>
      <StatusBar stats={sysStats} loading={sysLoading} />
      <main className="app-main">
        <AncestryPanel />
      </main>
    </div>
  );
}
