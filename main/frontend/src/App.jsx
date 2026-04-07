import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAppState, useAppDispatch } from './context.jsx';
import RawDataPanel from './components/RawDataPanel.jsx';
import ChatPanel from './components/ChatPanel.jsx';
import ServerPanel from './components/ServerPanel.jsx';
import ChecklistPanel from './components/ChecklistPanel.jsx';
import ReportsPanel from './components/ReportsPanel.jsx';
import UserManagementPanel from './components/UserManagementPanel.jsx';
import StatusBar from './components/StatusBar.jsx';
import useSystemStats from './hooks/useSystemStats.js';
import './App.css';

import Login from './components/Login';

const ADMIN_TAB = {
  label: 'Users',
  icon: 'M16 21v-2a4 4 0 00-4-4H6a4 4 0 00-4 4v2M9 11a4 4 0 100-8 4 4 0 000 8zM23 21v-2a4 4 0 00-3-3.87M16 3.13a4 4 0 010 7.75',
  adminOnly: true,
};

const BASE_TABS = [
  { label: 'AI Assistant', icon: 'M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z' },
  { label: 'Data & Pipeline', icon: 'M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7c-2 0-3 1-3 3zm5 2h6m-6 3h6m-6 3h4' },
  { label: 'Checklist', icon: 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h.01M9 16h.01M9 12h.01M13 16h.01M13 12h.01' },
  { label: 'PGS Runs', icon: 'M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4', route: '/PGSruns' },
  { label: 'Reports', icon: 'M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8zM14 2v6h6M16 13H8M16 17H8M10 9H8' },
  { label: 'Ancestry', icon: 'M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zM8 12s1-4 4-4 4 4 4 4-1 4-4 4-4-4-4-4z', route: '/ancestry' },
  { label: 'Server', icon: 'M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-7-4h.01M12 16h.01' },
];

function SiteLogo() {
  return (
    <div className="site-logo-icon" aria-label="23andClaude">
      <span className="logo-23">23</span>
      <span className="logo-and">&amp;</span>
      <span className="logo-claude">Claude</span>
    </div>
  );
}

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

  // All hooks must be called unconditionally (React rules of hooks)
  const { activeTab } = useAppState();
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const { stats: sysStats, loading: sysLoading, error: sysError } = useSystemStats(5000);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);

  // Close dropdown on outside click
  useEffect(() => {
    if (!menuOpen) return;
    const handleClick = (e) => {
      if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false);
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [menuOpen]);

  const handleLogout = () => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('refresh_token');
    setUser(null);
  };

  if (!authChecked) return null;
  if (!user) return <Login onLogin={(data) => setUser({ token: data.access_token, ...data.user })} />;

  // Tabs available to all users; the Users tab is appended only for admins.
  const isAdmin = user && user.role === 'admin';
  const TABS = isAdmin ? [...BASE_TABS, ADMIN_TAB] : BASE_TABS;

  // Build panel index map: skip route-based tabs (they navigate away)
  const panelComponents = [
    <ChatPanel key="chat" />,
    <RawDataPanel key="rawdata" />,
    <ChecklistPanel key="checklist" />,
    <ReportsPanel key="reports" />,
    <ServerPanel key="server" stats={sysStats} loading={sysLoading} error={sysError} />,
  ];
  if (isAdmin) {
    panelComponents.push(<UserManagementPanel key="users" currentUserId={user.id} />);
  }

  // Map tab index to panel index (route tabs don't have panels)
  const tabToPanelIndex = [];
  let pi = 0;
  for (const tab of TABS) {
    tabToPanelIndex.push(tab.route ? null : pi++);
  }

  const handleTabClick = (tab, i) => {
    if (tab.route) {
      navigate(tab.route);
    } else {
      dispatch({ type: 'SET_TAB', payload: tabToPanelIndex[i] });
    }
  };

  // Find which TABS index corresponds to current activeTab panel index
  const activeTabIndex = tabToPanelIndex.indexOf(activeTab);

  const handleMobileTabClick = (tab, i) => {
    handleTabClick(tab, i);
    setMenuOpen(false);
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-logo">
          <SiteLogo />
          <span className="app-title">23andClaude</span>
        </div>

        {/* Desktop tab nav */}
        <nav className="tab-nav tab-nav-desktop">
          {TABS.map((tab, i) => (
            <button
              key={tab.label}
              className={`tab-btn ${!tab.route && activeTabIndex === i ? 'active' : ''}`}
              onClick={() => handleTabClick(tab, i)}
            >
              <TabIcon d={tab.icon} />
              <span className="tab-label">{tab.label}</span>
            </button>
          ))}
        </nav>

        {/* Mobile hamburger + dropdown */}
        <div className="mobile-menu-wrap" ref={menuRef}>
          <button
            className={`hamburger-btn ${menuOpen ? 'open' : ''}`}
            onClick={() => setMenuOpen(!menuOpen)}
            aria-label="Menu"
          >
            <span /><span /><span />
          </button>
          {menuOpen && (
            <nav className="mobile-dropdown">
              {TABS.map((tab, i) => (
                <button
                  key={tab.label}
                  className={`mobile-dropdown-item ${!tab.route && activeTabIndex === i ? 'active' : ''}`}
                  onClick={() => handleMobileTabClick(tab, i)}
                >
                  <TabIcon d={tab.icon} />
                  <span>{tab.label}</span>
                </button>
              ))}
              <div className="mobile-dropdown-divider" />
              <button className="mobile-dropdown-item logout-item" onClick={handleLogout}>
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9" />
                </svg>
                <span>Logout</span>
              </button>
            </nav>
          )}
        </div>

        <button className="logout-btn-desktop" onClick={handleLogout}>Logout</button>
</header>
      <StatusBar stats={sysStats} loading={sysLoading} />
      <main className="app-main">
        {panelComponents[activeTab]}
      </main>
    </div>
  );
}

export default App;
