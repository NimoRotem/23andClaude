# Results & Reporting

## Output Structure

Each run produces files in `/data/runs/{run_id}/`:
```
scores.json           # All scores
run_report.md         # Human-readable report
{PGS_ID}_{sample}_detail.json  # Per-variant logs
```

## Score Interpretation

### Percentile
- Population-relative rank from empirical 1000G reference panel distribution
- Populations: EUR, AFR, EAS, SAS, AMR
- 50th = average; >95th = notably elevated; <5th = notably reduced

### Z-Score
- Standard deviations from population mean
- |Z| > 2 is noteworthy

### Match Rate
- gVCF: ~100% (includes reference-homozygous positions)
- Regular VCF: lower (only variant sites)
- BAM (Pipeline E+): high (reads at all positions)

### Confidence
- **high**: match_rate >= 80%, reliable frequencies
- **medium**: 50-80% match or mixed frequency sources
- **low**: <50% match or fallback frequencies

## Results JSON Format

```json
{
  "PGS000025": {
    "trait": "Alzheimer's disease",
    "raw_score": 0.123,
    "variants_matched": 18,
    "variants_total": 19,
    "match_rate": 0.947,
    "z_score": 1.012,
    "percentile": 84.4,
    "ref_population": "EUR",
    "freq_source": "1kg_plink2",
    "confidence": "high"
  }
}
```

## Fetching Results

```bash
curl http://localhost:8600/api/runs/{run_id}/results
curl http://localhost:8600/api/runs/{run_id}/results/raw/run_report.md
```
