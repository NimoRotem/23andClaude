import { useAppState, useAppDispatch } from '../context.jsx';
import ScorePanel from './ScorePanel.jsx';
import ResultsPanel from './ResultsPanel.jsx';

export default function PGSRunsPanel() {
  const { pgsSubView } = useAppState();
  const dispatch = useAppDispatch();
  const view = pgsSubView || 'score';

  const setView = (v) => dispatch({ type: 'SET_PGS_VIEW', payload: v });

  return (
    <div>
      {/* Sub-navigation */}
      <div style={{
        display: 'flex', gap: 4, marginBottom: 16, padding: 3,
        background: '#161b22', borderRadius: 8, border: '1px solid #30363d',
        width: 'fit-content',
      }}>
        <button onClick={() => setView('score')} style={{
          padding: '7px 18px', borderRadius: 6, border: 'none', cursor: 'pointer',
          fontSize: 13, fontWeight: 600, transition: 'all 0.15s',
          background: view === 'score' ? '#21262d' : 'transparent',
          color: view === 'score' ? '#e6edf3' : '#8b949e',
        }}>
          New Run
        </button>
        <button onClick={() => setView('results')} style={{
          padding: '7px 18px', borderRadius: 6, border: 'none', cursor: 'pointer',
          fontSize: 13, fontWeight: 600, transition: 'all 0.15s',
          background: view === 'results' ? '#21262d' : 'transparent',
          color: view === 'results' ? '#e6edf3' : '#8b949e',
        }}>
          Results
        </button>
      </div>

      {view === 'score' && <ScorePanel />}
      {view === 'results' && <ResultsPanel />}
    </div>
  );
}
