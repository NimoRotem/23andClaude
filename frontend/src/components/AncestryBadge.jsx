/**
 * AncestryBadge — shows HIGH / MODERATE / LOW confidence for a PGS score.
 *
 * Props:
 *   confidence: "high" | "moderate" | "low"
 *   scoringMethod: string (e.g. "PRSCSx_combined")
 *   referencePopulation: string
 *   referenceN: number
 *   coveredFraction: number (0-1)
 *   ancestryWarnings: string[]
 *   compact: boolean (default false) — show just the badge dot
 */

import { useState } from 'react';

const CONF = {
  high: { color: '#3fb950', bg: 'rgba(63,185,80,0.12)', border: 'rgba(63,185,80,0.3)', label: 'HIGH CONFIDENCE', dot: '\u{1F7E2}' },
  moderate: { color: '#d29922', bg: 'rgba(210,153,34,0.12)', border: 'rgba(210,153,34,0.3)', label: 'MODERATE', dot: '\u{1F7E1}' },
  low: { color: '#f85149', bg: 'rgba(248,81,73,0.12)', border: 'rgba(248,81,73,0.3)', label: 'LOW CONFIDENCE', dot: '\u{1F534}' },
};

export default function AncestryBadge({
  confidence = 'low',
  scoringMethod,
  referencePopulation,
  referenceN,
  coveredFraction,
  ancestryWarnings = [],
  compact = false,
}) {
  const [expanded, setExpanded] = useState(false);
  const c = CONF[confidence] || CONF.low;

  if (compact) {
    return (
      <span title={`${c.label} — ${scoringMethod || ''}`} style={{
        display: 'inline-block', width: 8, height: 8, borderRadius: '50%',
        background: c.color, cursor: 'help',
      }} />
    );
  }

  return (
    <div style={{
      display: 'inline-flex', flexDirection: 'column', gap: 4,
      background: c.bg, border: `1px solid ${c.border}`, borderRadius: 6,
      padding: '6px 10px', fontSize: 12,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{ width: 8, height: 8, borderRadius: '50%', background: c.color }} />
        <span style={{ color: c.color, fontWeight: 700, fontSize: 11, letterSpacing: 0.5 }}>{c.label}</span>
        <span
          onClick={() => setExpanded(!expanded)}
          style={{
            marginLeft: 4, width: 16, height: 16, borderRadius: '50%',
            background: 'rgba(139,148,158,0.15)', color: '#8b949e',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: 10, cursor: 'pointer', fontWeight: 700,
          }}
          title="Show details">
          i
        </span>
      </div>

      {scoringMethod && (
        <div style={{ color: '#8b949e', fontSize: 11 }}>
          {scoringMethod.replace(/_/g, ' ')}
        </div>
      )}

      {referencePopulation && (
        <div style={{ color: '#8b949e', fontSize: 11 }}>
          Ref: {referencePopulation}{referenceN ? ` (n=${referenceN.toLocaleString()})` : ''}
        </div>
      )}

      {coveredFraction != null && (
        <div style={{ color: '#8b949e', fontSize: 11 }}>
          Ancestry coverage: {(coveredFraction * 100).toFixed(0)}%
        </div>
      )}

      {expanded && ancestryWarnings.length > 0 && (
        <div style={{ marginTop: 4, padding: '6px 8px', background: 'rgba(0,0,0,0.2)', borderRadius: 4 }}>
          {ancestryWarnings.map((w, i) => (
            <div key={i} style={{ color: '#d29922', fontSize: 11, display: 'flex', gap: 4 }}>
              <span>⚠️</span><span>{w}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
