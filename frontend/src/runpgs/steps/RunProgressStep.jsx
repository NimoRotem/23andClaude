import { useEffect, useRef, useCallback } from 'react';
import { connectRunProgress, runApi } from '../../api.js';
import { useRunPGS, useRunPGSDispatch } from '../RunPGSState.jsx';
import { formatElapsed, isDone } from '../utils.js';

export default function RunProgressStep() {
  const { activeRunId, progress, elapsed, runStartedAt, recentRuns } = useRunPGS();
  const dispatch = useRunPGSDispatch();
  const wsRef = useRef(null);
  const pollRef = useRef(null);

  const showToast = (msg, type = 'success') =>
    dispatch({ type: 'SHOW_TOAST', payload: { msg, type } });

  function refreshRuns() {
    runApi.list().then(data => {
      const all = Array.isArray(data) ? data : (data?.runs || []);
      dispatch({ type: 'SET_RECENT_RUNS', payload: all.slice(0, 10) });
    }).catch(() => {});
  }

  const pollRunState = useCallback(async (runId) => {
    try {
      const run = await fetch(`/api/runs/${runId}`).then(r => r.json());
      if (run.started_at && !runStartedAt) {
        dispatch({ type: 'SET_RUN_STARTED_AT', payload: new Date(run.started_at).getTime() });
      }
      dispatch({ type: 'UPDATE_PROGRESS', payload: {
        status: run.status,
        pct: run.progress_pct || 0,
        step: run.current_step || run.status,
        error: run.error_message,
        run_detail: run,
      }});
      return run;
    } catch { return null; }
  }, [dispatch, runStartedAt]);

  // Elapsed timer
  useEffect(() => {
    if (!runStartedAt || (progress && isDone(progress.status))) return;
    const tick = () => dispatch({ type: 'SET_ELAPSED', payload: Math.max(0, Math.round((Date.now() - runStartedAt) / 1000)) });
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [runStartedAt, progress?.status, dispatch]);

  // WebSocket + polling
  useEffect(() => {
    if (!activeRunId) return;

    // Init progress
    if (!progress) {
      dispatch({ type: 'SET_PROGRESS', payload: { overall: 0, step: '', pgs_progress: [], status: 'running', pct: 0, run_detail: null } });
    }

    pollRunState(activeRunId).then(run => {
      if (run && isDone(run.status)) refreshRuns();
    });

    const ws = connectRunProgress(
      activeRunId,
      (data) => {
        dispatch({ type: 'UPDATE_PROGRESS', payload: data });
        if (!runStartedAt && data.status === 'scoring') {
          dispatch({ type: 'SET_RUN_STARTED_AT', payload: Date.now() });
        }
        if (isDone(data.status)) refreshRuns();
      },
      () => pollRunState(activeRunId).then(() => refreshRuns())
    );
    wsRef.current = ws;

    pollRef.current = setInterval(() => {
      pollRunState(activeRunId).then(run => {
        if (run && isDone(run.status)) clearInterval(pollRef.current);
      });
    }, 5000);

    return () => {
      if (wsRef.current) wsRef.current.close();
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [activeRunId]);

  if (!activeRunId) {
    return (
      <div className="rpgs-section" style={{ textAlign: 'center', padding: 40 }}>
        <div style={{ fontSize: 48, opacity: 0.3, marginBottom: 16 }}>&#x23F3;</div>
        <p style={{ color: '#8b949e', fontSize: 15 }}>No active run. Go back and start a scoring run.</p>
        <button className="btn" style={{ marginTop: 16 }} onClick={() => dispatch({ type: 'SET_STEP', payload: 2 })}>
          &larr; Back to Configure
        </button>
      </div>
    );
  }

  const p = progress || {};
  const done = isDone(p.status);
  const failed = p.status === 'failed';

  return (
    <div>
      <div className="rpgs-section">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
          <h3 className="rpgs-section-title" style={{ margin: 0 }}>
            {done ? (failed ? 'Run Failed' : 'Run Completed') : 'Scoring in Progress...'}
          </h3>
          <span style={{ fontSize: 20, fontWeight: 700, color: '#8b949e', fontVariantNumeric: 'tabular-nums', fontFamily: 'monospace' }}>
            {formatElapsed(elapsed)}
          </span>
        </div>

        {/* Run info */}
        {p.run_detail && (
          <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 12, display: 'flex', gap: 16, flexWrap: 'wrap' }}>
            <span>PGS: <strong style={{ color: '#c9d1d9' }}>{(p.run_detail.pgs_ids || []).join(', ')}</strong></span>
            <span>Files: <strong style={{ color: '#c9d1d9' }}>{(p.run_detail.source_files || []).map(f => f.filename || f.path?.split('/').pop()).join(', ')}</strong></span>
          </div>
        )}

        {/* Status line */}
        <div style={{ fontSize: 13, color: '#c9d1d9', marginBottom: 12 }}>
          Status: <strong>{p.status || 'running'}</strong>
          {(p.step_name || p.step) && (
            <> &middot; <strong>{p.step_name || p.step}</strong></>
          )}
        </div>

        {/* Overall progress bar */}
        <div className="progress-bar-track progress-blue" style={{ height: 20, marginBottom: 6 }}>
          <div className="progress-bar-fill" style={{ width: `${p.pct ?? p.overall ?? 0}%` }} />
        </div>
        <div style={{ fontSize: 13, color: '#c9d1d9', textAlign: 'right', marginBottom: 16, fontWeight: 600 }}>
          {Math.round(p.pct ?? p.overall ?? 0)}%
        </div>

        {/* Per-PGS sub-progress */}
        {p.pgs_progress && p.pgs_progress.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {p.pgs_progress.map(pg => (
              <div key={pg.id || pg.pgs_id} style={{ display: 'flex', alignItems: 'center', gap: 10, fontSize: 12 }}>
                <span style={{ color: '#c9d1d9', minWidth: 120, flexShrink: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {pg.id || pg.pgs_id}
                </span>
                <div style={{ flex: 1 }}>
                  <div className="progress-bar-track progress-green" style={{ height: 8 }}>
                    <div className="progress-bar-fill" style={{ width: `${pg.progress ?? 0}%` }} />
                  </div>
                </div>
                <span style={{ color: '#8b949e', minWidth: 36, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                  {Math.round(pg.progress ?? 0)}%
                </span>
              </div>
            ))}
          </div>
        )}

        {/* Error */}
        {failed && (p.error || p.error_message) && (
          <div style={{
            marginTop: 16, padding: 14, background: 'rgba(248,81,73,0.08)',
            borderRadius: 8, border: '1px solid rgba(248,81,73,0.2)', color: '#f85149', fontSize: 13,
          }}>
            <strong>Error:</strong> {p.error || p.error_message}
          </div>
        )}

        {/* Completed -> view results */}
        {done && !failed && (
          <div style={{ marginTop: 20, textAlign: 'center' }}>
            <button className="btn btn-primary" style={{ fontSize: 15, padding: '10px 28px' }}
              onClick={() => dispatch({ type: 'GO_TO_RESULTS', payload: activeRunId })}>
              View Results &rarr;
            </button>
          </div>
        )}

        {/* Failed -> retry */}
        {failed && (
          <div style={{ marginTop: 16, textAlign: 'center' }}>
            <button className="btn" onClick={() => dispatch({ type: 'SET_STEP', payload: 2 })}>
              &larr; Back to Configure
            </button>
          </div>
        )}
      </div>

      {/* Recent runs */}
      {recentRuns.length > 0 && (
        <div className="rpgs-section">
          <h3 className="rpgs-section-title">Recent Runs</h3>
          {recentRuns.slice(0, 5).map(run => (
            <div key={run.id} className="rpgs-run-item"
              onClick={() => {
                if (run.status === 'completed' || run.status === 'complete') {
                  dispatch({ type: 'GO_TO_RESULTS', payload: run.id });
                }
              }}
            >
              <div>
                <span style={{ fontSize: 13, color: '#c9d1d9', fontWeight: 600 }}>Run #{run.id}</span>
                <span style={{ fontSize: 11, color: '#8b949e', marginLeft: 8 }}>
                  {run.pgs_ids?.length || '?'} PGS &middot; {run.started_at || ''}
                </span>
              </div>
              <span className={`badge badge-${
                run.status === 'completed' || run.status === 'complete' ? 'green' :
                run.status === 'failed' ? 'red' :
                run.status === 'running' ? 'blue' : 'gray'
              }`}>
                {run.status}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
