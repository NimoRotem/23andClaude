import PGSSelector from '../../components/PGSSelector.jsx';
import { useRunPGS, useRunPGSDispatch } from '../RunPGSState.jsx';

export default function PGSSelectionStep() {
  const { selectedPgsIds } = useRunPGS();
  const dispatch = useRunPGSDispatch();

  return (
    <div>
      <div className="rpgs-section">
        <h3 className="rpgs-section-title">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#58a6ff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35" />
          </svg>
          Search & Select PGS Scores
          {selectedPgsIds.length > 0 && (
            <span className="badge badge-blue" style={{ marginLeft: 4 }}>
              {selectedPgsIds.length} selected
            </span>
          )}
        </h3>
        <PGSSelector
          selectedPgsIds={selectedPgsIds}
          onAdd={(pgs) => dispatch({ type: 'ADD_PGS', payload: pgs })}
          onRemove={(id) => dispatch({ type: 'REMOVE_PGS', payload: id })}
          onClear={() => dispatch({ type: 'CLEAR_PGS' })}
        />
      </div>

      <div className="rpgs-step-nav">
        <button className="btn" onClick={() => dispatch({ type: 'SET_STEP', payload: 0 })}>
          &larr; Back
        </button>
        <button
          className="btn btn-primary"
          disabled={selectedPgsIds.length === 0}
          onClick={() => dispatch({ type: 'SET_STEP', payload: 2 })}
        >
          Continue — Configure &rarr;
        </button>
      </div>
    </div>
  );
}
