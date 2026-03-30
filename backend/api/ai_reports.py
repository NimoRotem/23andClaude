"""AI-powered report generation using Claude API.

Generates meaningful, plain-language genomics reports by interpreting
raw analysis output through an LLM. Falls back gracefully if no API key.
"""

import logging
import os

from backend.config import ANTHROPIC_API_KEY, AI_REPORT_MODEL

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    """Lazy-init Anthropic client."""
    global _client
    if _client is None and ANTHROPIC_API_KEY:
        try:
            from anthropic import Anthropic
            _client = Anthropic(api_key=ANTHROPIC_API_KEY)
        except Exception as e:
            logger.warning(f"Failed to init Anthropic client: {e}")
    return _client


def write_ai_report(
    analysis_type: str,
    check_name: str,
    sample_name: str,
    raw_output: str,
    static_interpretation: str = "",
    extra_context: str = "",
) -> str | None:
    """Generate an AI-written narrative for a genomics analysis result.

    Returns markdown string, or None if AI is unavailable.
    """
    client = _get_client()
    if not client:
        return None

    prompt = _build_prompt(analysis_type, check_name, sample_name,
                           raw_output, static_interpretation, extra_context)

    try:
        response = client.messages.create(
            model=AI_REPORT_MODEL,
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"AI report generation failed: {e}")
        return None


def write_ai_pgs_report(
    pgs_id: str,
    trait: str,
    scores: list[dict],
    match_rate: float,
    variants_total: int,
    study_info: str = "",
    population: str = "EUR",
) -> str | None:
    """Generate AI narrative for a PGS report."""
    client = _get_client()
    if not client:
        return None

    scores_text = "\n".join(
        f"- {s['sample']}: percentile={s.get('percentile', '?')}%, Z-score={s.get('z_score', '?')}, raw={s.get('raw_score', '?')}"
        for s in scores
    )

    prompt = f"""You are a clinical genetics report writer creating a patient-friendly polygenic risk score report.

## Analysis Details
- PGS ID: {pgs_id}
- Trait/Disease: {trait}
- Study Population: {population}
- Variant Match Rate: {match_rate*100:.1f}%
- Total Variants in Score: {variants_total:,}
{f'- Study: {study_info}' if study_info else ''}

## Per-Sample Results
{scores_text}

## Instructions
Write a clear, meaningful report with these sections:

### Summary
2-3 sentences: What was measured and the key finding for each sample. Use plain language.

### What This Means
Explain what the percentile means in practical terms. For example:
- 90th+ percentile = top 10% of genetic risk, significantly elevated
- 75-90th = above average risk
- 25-75th = average range
- Below 25th = below average genetic risk

For each sample, explain their specific risk level and what it means for them.

### Reliability
Comment on:
- Match rate ({match_rate*100:.1f}%) — is this result reliable? (>95% = excellent, 80-95% = good, <80% = interpret with caution)
- Whether the study population ({population}) matches the sample's likely ancestry
- General limitations of polygenic scores (they capture statistical risk, not certainty)

### Recommended Actions
Based on the risk level, suggest appropriate next steps (screening, lifestyle, genetic counseling, etc.). Be specific to the disease/trait.

Keep the tone professional but accessible. No jargon without explanation. Write in markdown format."""

    try:
        response = client.messages.create(
            model=AI_REPORT_MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"AI PGS report failed: {e}")
        return None


def write_ai_sample_summary(
    sample_name: str,
    sections: dict[str, list[dict]],
    pgs_results: list[dict],
) -> str | None:
    """Generate AI narrative for a per-sample summary report."""
    client = _get_client()
    if not client:
        return None

    # Build context from all sections
    context_parts = []
    for cat, items in sections.items():
        if not items:
            continue
        context_parts.append(f"\n### {cat}")
        for item in items:
            interp = item.get("interpretation") or item.get("output", "")[:200]
            context_parts.append(f"- {item['check']}: {interp}")

    if pgs_results:
        context_parts.append("\n### Polygenic Risk Scores (sorted by risk, highest first)")
        for p in pgs_results[:20]:
            pct = p.get("percentile")
            if pct is not None:
                risk = "HIGH" if pct >= 90 else "Above Avg" if pct >= 75 else "Average" if pct >= 25 else "Below Avg"
                context_parts.append(f"- {p.get('item_id', '?')}: {pct:.1f}th percentile ({risk})")

    context = "\n".join(context_parts)

    prompt = f"""You are a clinical genetics report writer creating an executive summary for a whole-genome sequencing analysis.

## Sample: {sample_name}

## All Available Results
{context}

## Instructions
Write a comprehensive but readable executive summary with these sections:

### Overview
1-2 paragraphs summarizing the key findings across all analyses. Lead with the most important/actionable findings.

### Sex Determination
Clear statement of biological sex based on the evidence (Y reads, SRY, X:Y ratio).

### Sample Quality
Brief assessment: is this a high-quality sample suitable for clinical interpretation?

### Notable Genetic Risk Factors
Highlight any PGS scores in the high-risk range (>90th percentile) or significant single-variant findings. Explain what each means.

### Ancestry & Population Context
Summarize ancestry findings and note any implications for PGS interpretation.

### Pharmacogenomics Highlights
If any PGx results are available, note key drug-gene interactions.

### Recommendations
Top 3-5 actionable recommendations based on all findings combined.

Write in professional but accessible language. Use markdown formatting."""

    try:
        response = client.messages.create(
            model=AI_REPORT_MODEL,
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"AI sample summary failed: {e}")
        return None


def _build_prompt(analysis_type, check_name, sample_name, raw_output, static_interpretation, extra_context):
    """Build prompt for command-based analysis reports."""
    return f"""You are a clinical genetics report writer. Generate a clear, meaningful interpretation of this genomics analysis result.

## Analysis
- Type: {analysis_type}
- Check: {check_name}
- Sample: {sample_name}

## Raw Output
```
{raw_output[:2000]}
```

{f'## Existing Interpretation (enhance this){chr(10)}{static_interpretation}' if static_interpretation else ''}

{f'## Additional Context{chr(10)}{extra_context}' if extra_context else ''}

## Instructions
Write a brief (2-3 paragraphs) report explaining:

1. **What was tested**: Explain in one sentence what this analysis checks and why it matters.
2. **What the result shows**: Interpret the raw output in plain language. If this is a genotype (like 0/1 or 1/1), explain what each allele means.
3. **Clinical significance**: Is this normal? Should the person be concerned? What actions (if any) should they consider?

Keep it concise, professional, and accessible to someone without genetics training. Use markdown formatting. Do NOT include raw command output or technical details — focus on meaning."""
