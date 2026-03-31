const STEPS = [
  { label: 'Files', icon: 'M13 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V9z' },
  { label: 'PGS Scores', icon: 'M11 11a4 4 0 100-8 4 4 0 000 8zm0 0l4.35 4.35' },
  { label: 'Configure', icon: 'M12 15V3m-6 8l6 6 6-6' },
  { label: 'Running', icon: 'M5 3l14 9-14 9V3z' },
  { label: 'Results', icon: 'M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z' },
];

export default function StepIndicator({ current, maxVisited, onStep }) {
  return (
    <div className="rpgs-stepper">
      {STEPS.map((step, i) => {
        const done = i < current;
        const active = i === current;
        const clickable = i <= maxVisited;
        return (
          <div key={i} className="rpgs-stepper-item">
            {i > 0 && (
              <div className={`rpgs-stepper-line ${done ? 'done' : ''}`} />
            )}
            <button
              className={`rpgs-stepper-circle ${active ? 'active' : ''} ${done ? 'done' : ''}`}
              disabled={!clickable}
              onClick={() => clickable && onStep(i)}
              title={step.label}
            >
              {done ? (
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="20 6 9 17 4 12" />
                </svg>
              ) : (
                <span>{i + 1}</span>
              )}
            </button>
            <span className={`rpgs-stepper-label ${active ? 'active' : ''} ${done ? 'done' : ''}`}>
              {step.label}
            </span>
          </div>
        );
      })}
    </div>
  );
}
