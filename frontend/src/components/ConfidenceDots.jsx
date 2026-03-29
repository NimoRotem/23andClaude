/**
 * ConfidenceDots — shows 6 colored dots (one per sample) indicating
 * confidence level for a trait/PGS across all samples.
 *
 * Props:
 *   sampleConfidences: [{sample_id, confidence}] — one per sample, fixed order
 *   size: number (default 8) — dot diameter
 */

const CONF_COLORS = {
  high: '#3fb950',
  moderate: '#d29922',
  low: '#f85149',
  unknown: '#484f58',
};

export default function ConfidenceDots({ sampleConfidences = [], size = 8 }) {
  return (
    <span style={{ display: 'inline-flex', gap: 3, alignItems: 'center' }}>
      {sampleConfidences.map((sc, i) => (
        <span
          key={sc.sample_id || i}
          title={`${sc.sample_id}: ${(sc.confidence || 'unknown').toUpperCase()}`}
          style={{
            display: 'inline-block',
            width: size,
            height: size,
            borderRadius: '50%',
            background: CONF_COLORS[sc.confidence] || CONF_COLORS.unknown,
            cursor: 'help',
          }}
        />
      ))}
    </span>
  );
}
