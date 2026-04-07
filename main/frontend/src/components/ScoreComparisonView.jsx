/**
 * ScoreComparisonView — expandable per-trait comparison showing all scoring methods.
 *
 * Props:
 *   trait: string — trait name
 *   pgsId: string — PGS ID
 *   sampleId: string — sample ID
 *   sampleAncestry: string — e.g. "EUR", "EUR/EAS admixed"
 *   results: [{scoring_method, percentile, confidence, reference_population, ...}]
 *   standardResult: {percentile, reference_population, ...} — standard PGS Catalog result
 */

import { useState } from 'react';
import AncestryBadge from './AncestryBadge.jsx';

const METHOD_LABELS = {
  PRSCSx_combined: 'PRS-CSx combined',
  PRSCSx_EUR: 'EUR component',
  PRSCSx_EAS: 'EAS component',
  PRSCSx_AFR: 'AFR component',
  PRSCSx_SAS: 'SAS component',
  PRSCSx_AMR: 'AMR component',
  plink2_standard: 'Standard PGS',
};

function pctColor(pct) {
  if (pct == null) return '#484f58';
  if (pct >= 90) return '#f85149';
  if (pct >= 75) return '#d29922';
  if (pct >= 25) return '#3fb950';
  if (pct >= 10) return '#d29922';
  return '#58a6ff';
}

export default function ScoreComparisonView({
  trait,
  pgsId,
  sampleId,
  sampleAncestry,
  results = [],
  standardResult,
}) {
  const [expanded, setExpanded] = useState(false);

  // Find the combined result (primary display)
  const combined = results.find(r => r.scoring_method === 'PRSCSx_combined');
  const components = results.filter(r => r.scoring_method !== 'PRSCSx_combined');

  const hasAncestryScores = results.length > 0;

  return (
    <div style={{
      background: '#161b22', border: '1px solid #21262d', borderRadius: 8,
      padding: '12px 16px', marginBottom: 8,
    }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}
        onClick={() => setExpanded(!expanded)}>
        <span style={{ color: '#484f58', fontSize: 11, width: 12 }}>
          {expanded ? '\u25BC' : '\u25B6'}
        </span>
        <span style={{ color: '#e6edf3', fontWeight: 600, fontSize: 13 }}>
          {trait}
        </span>
        {pgsId && (
          <span style={{ color: '#6e7681', fontSize: 11 }}>({pgsId})</span>
        )}
        <span style={{ color: '#8b949e', fontSize: 12, marginLeft: 4 }}>
          — {sampleId}
          <span style={{ marginLeft: 4, color: '#6e7681' }}>({sampleAncestry})</span>
        </span>

        {/* Quick percentile + badge */}
        {combined && (
          <span style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ color: pctColor(combined.percentile), fontWeight: 700, fontFamily: 'monospace', fontSize: 14 }}>
              {combined.percentile != null ? `${combined.percentile.toFixed(0)}th` : '--'}
            </span>
            <AncestryBadge confidence={combined.confidence} compact />
          </span>
        )}
      </div>

      {/* Expanded comparison */}
      {expanded && (
        <div style={{ marginTop: 12, paddingLeft: 20, borderLeft: '2px solid #21262d' }}>
          {/* Combined score */}
          {combined && (
            <ScoreRow
              label="PRS-CSx combined"
              percentile={combined.percentile}
              confidence={combined.confidence}
              refPop={combined.reference_population}
              refN={combined.reference_n}
              coveredFraction={combined.covered_fraction}
              scoringMethod={combined.scoring_method}
              warnings={combined.ancestry_warnings}
              isMain
            />
          )}

          {/* Component scores */}
          {components.map(r => (
            <ScoreRow
              key={r.scoring_method}
              label={METHOD_LABELS[r.scoring_method] || r.scoring_method}
              percentile={r.percentile}
              confidence={r.confidence}
              refPop={r.reference_population}
              refN={r.reference_n}
            />
          ))}

          {/* Standard PGS result */}
          {standardResult && (
            <ScoreRow
              label={`Standard ${pgsId || 'PGS'}`}
              percentile={standardResult.percentile}
              confidence={standardResult.confidence || (standardResult.pgs_training_pop_match === false ? 'low' : 'high')}
              refPop={standardResult.reference_population}
              refN={standardResult.reference_n}
              mismatchWarning={standardResult.pgs_training_pop_match === false}
            />
          )}

          {!hasAncestryScores && !standardResult && (
            <div style={{ color: '#6e7681', fontSize: 12, padding: '4px 0' }}>
              No ancestry-aware scores available for this trait.
            </div>
          )}
        </div>
      )}
    </div>
  );
}


function ScoreRow({ label, percentile, confidence, refPop, refN, coveredFraction, scoringMethod, warnings, isMain, mismatchWarning }) {
  const conf = {
    high: { color: '#3fb950', dot: '\u{1F7E2}' },
    moderate: { color: '#d29922', dot: '\u{1F7E1}' },
    low: { color: '#f85149', dot: '\u{1F534}' },
  };
  const c = conf[confidence] || conf.low;

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 10, padding: '6px 0',
      borderBottom: '1px solid #161b22', fontSize: 12,
    }}>
      <span style={{ color: '#484f58', fontSize: 10 }}>{isMain ? '\u251C\u2500\u2500' : '\u251C\u2500\u2500'}</span>
      <span style={{ color: isMain ? '#e6edf3' : '#c9d1d9', fontWeight: isMain ? 600 : 400, minWidth: 140 }}>
        {label}:
      </span>
      <span style={{
        color: pctColor(percentile), fontWeight: 700, fontFamily: 'monospace',
        minWidth: 100,
      }}>
        {percentile != null ? `${percentile.toFixed(0)}th percentile` : '--'}
      </span>
      {refPop && (
        <span style={{ color: '#6e7681', fontSize: 11 }}>
          (ref: {refPop}{refN ? `, n=${refN.toLocaleString()}` : ''})
        </span>
      )}
      <span style={{ width: 8, height: 8, borderRadius: '50%', background: c.color, marginLeft: 4 }}
        title={confidence?.toUpperCase()} />
      {mismatchWarning && (
        <span style={{ color: '#f85149', fontSize: 11, display: 'flex', alignItems: 'center', gap: 3 }}
          title="This score was trained in European populations. Percentile ranking against a European reference may not be accurate for this sample's ancestry.">
          ⚠️ ancestry mismatch
        </span>
      )}
    </div>
  );
}
