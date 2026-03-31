import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { RunPGSProvider, useRunPGS, useRunPGSDispatch } from './RunPGSState.jsx';
import StepIndicator from './components/StepIndicator.jsx';
import FileSelectionStep from './steps/FileSelectionStep.jsx';
import PGSSelectionStep from './steps/PGSSelectionStep.jsx';
import ConfigureStep from './steps/ConfigureStep.jsx';
import RunProgressStep from './steps/RunProgressStep.jsx';
import ResultsStep from './steps/ResultsStep.jsx';
import Login from '../components/Login.jsx';
import '../App.css';
import './RunPGSApp.css';

function RunPGSInner() {
  const state = useRunPGS();
  const dispatch = useRunPGSDispatch();
  const { currentStep, maxVisitedStep, toast } = state;

  const steps = [
    <FileSelectionStep key="files" />,
    <PGSSelectionStep key="pgs" />,
    <ConfigureStep key="config" />,
    <RunProgressStep key="progress" />,
    <ResultsStep key="results" />,
  ];

  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => dispatch({ type: 'HIDE_TOAST' }), 4000);
    return () => clearTimeout(t);
  }, [toast, dispatch]);

  return (
    <div className="rpgs-page">
      <header className="rpgs-header">
        <Link to="/" className="rpgs-header-back">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 18 9 12 15 6" />
          </svg>
          Dashboard
        </Link>
        <div className="rpgs-header-title">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2">
            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2z"/>
            <path d="M8 12s1-4 4-4 4 4 4 4-1 4-4 4-4-4-4-4z"/>
          </svg>
          23andClaude
        </div>
        <button className="rpgs-header-logout" onClick={() => {
          localStorage.removeItem('auth_token');
          localStorage.removeItem('refresh_token');
          window.location.reload();
        }}>
          Logout
        </button>
      </header>

      <StepIndicator
        current={currentStep}
        maxVisited={maxVisitedStep}
        onStep={(i) => dispatch({ type: 'SET_STEP', payload: i })}
      />

      <div className="rpgs-content">
        {steps[currentStep]}
      </div>

      {toast && (
        <div className={`rpgs-toast ${toast.type || 'success'}`}>
          {toast.msg}
        </div>
      )}
    </div>
  );
}

export default function RunPGSApp() {
  const [user, setUser] = useState(() => {
    const token = localStorage.getItem('auth_token');
    return token ? { token } : null;
  });
  const [authChecked, setAuthChecked] = useState(false);

  useEffect(() => {
    const token = localStorage.getItem('auth_token');
    if (!token) { setAuthChecked(true); return; }
    fetch('/genomics/api/auth/me', { headers: { Authorization: 'Bearer ' + token } })
      .then(r => { if (!r.ok) throw new Error(); return r.json(); })
      .then(data => { setUser({ token, ...data }); setAuthChecked(true); })
      .catch(() => { localStorage.removeItem('auth_token'); setUser(null); setAuthChecked(true); });
  }, []);

  if (!authChecked) return null;
  if (!user) return <Login onLogin={(data) => setUser({ token: data.access_token, ...data.user })} />;

  return (
    <RunPGSProvider>
      <RunPGSInner />
    </RunPGSProvider>
  );
}
