import React, { useState } from 'react';
import { useAppState, useAppDispatch } from './context.jsx';
import RawDataPanel from './components/RawDataPanel.jsx';
import PGSRunsPanel from './components/PGSRunsPanel.jsx';
import ChatPanel from './components/ChatPanel.jsx';
import ServerPanel from './components/ServerPanel.jsx';
import ChecklistPanel from './components/ChecklistPanel.jsx';
import ReportsPanel from './components/ReportsPanel.jsx';
import StatusBar from './components/StatusBar.jsx';
import useSystemStats from './hooks/useSystemStats.js';
import './App.css';

import Login from './components/Login';
const TABS = [
  { label: 'AI Assistant', icon: 'M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z' },
  { label: 'Data & Pipeline', icon: 'M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7c-2 0-3 1-3 3zm5 2h6m-6 3h6m-6 3h4' },
  { label: 'Checklist', icon: 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h.01M9 16h.01M9 12h.01M13 16h.01M13 12h.01' },
  { label: 'PGS Runs', icon: 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4' },
  { label: 'Reports', icon: 'M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8zM14 2v6h6M16 13H8M16 17H8M10 9H8' },
  { label: 'Server', icon: 'M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-7-4h.01M12 16h.01' },
];

function TabIcon({ d }) {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d={d} />
    </svg>
  );
}

function App() {
  const [user, setUser] = useState(() => {
    const token = localStorage.getItem('auth_token');
    return token ? { token } : null;
  });

  // Verify token on mount
  const [authChecked, setAuthChecked] = useState(false);
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

  const { activeTab } = useAppState();
  const dispatch = useAppDispatch();
  const { stats: sysStats, loading: sysLoading, error: sysError } = useSystemStats(5000);

  const panels = [
    <ChatPanel key="chat" />,
    <RawDataPanel key="rawdata" />,
    <ChecklistPanel key="checklist" />,
    <PGSRunsPanel key="pgs-runs" />,
    <ReportsPanel key="reports" />,
    <ServerPanel key="server" stats={sysStats} loading={sysLoading} error={sysError} />,
  ];

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-logo">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z"/>
            <path d="M8 12s1-4 4-4 4 4 4 4-1 4-4 4-4-4-4-4z"/>
          </svg>
          <span className="app-title">23andClaude</span>
        </div>
        <nav className="tab-nav">
          {TABS.map((tab, i) => (
            <button
              key={tab.label}
              className={`tab-btn ${activeTab === i ? 'active' : ''}`}
              onClick={() => dispatch({ type: 'SET_TAB', payload: i })}
            >
              <TabIcon d={tab.icon} />
              <span className="tab-label">{tab.label}</span>
            </button>
          ))}
        </nav>

        <button onClick={handleLogout} style={{
          background: 'transparent', border: '1px solid #30363d', color: '#8b949e',
          padding: '4px 12px', borderRadius: 6, cursor: 'pointer', fontSize: 12,
          marginLeft: 'auto', marginRight: 12,
        }}>Logout</button>
</header>
      <StatusBar stats={sysStats} loading={sysLoading} />
      <main className="app-main">
        {panels[activeTab]}
      </main>
    </div>
  );
}

export default App;
