# PGS Data Inventory

## Cached PGS Scoring Files

All in `/data/pgs_cache/{PGS_ID}/` as harmonized GRCh38 `.txt.gz`.

| PGS ID | Trait | Variants |
|--------|-------|----------|
| PGS000016 | Atrial fibrillation | 6,730,541 |
| PGS000025 | Alzheimer's disease | 19 |
| PGS000026 | Alzheimer's disease | 33 |
| PGS000035 | Atrial fibrillation | 1,168 |
| PGS000119 | Basal cell carcinoma | 32 |
| PGS000198 | Psoriatic arthritis | 31 |
| PGS000297 | Height | 3,290 |
| PGS000327 | Autism spectrum disorder | 35,087 |
| PGS000338 | Atrial fibrillation | 97 |
| PGS000342 | Psoriatic arthritis | 11 |
| PGS000343 | Psoriatic arthritis | 5 |
| PGS000381 | Colon cancer | 12 |
| PGS000382 | Colon cancer | 150 |
| PGS000446 | Basal cell carcinoma | 1,111,490 |
| PGS000451 | Basal cell carcinoma | 2,231 |
| PGS000711 | Gout | 183,332 |
| PGS000758 | Adult standing height | 33,938 |
| PGS000779 | Alzheimer's disease | 7 |
| PGS000811 | Alzheimer's disease | 39 |
| PGS000812 | Alzheimer's disease | 57 |
| PGS000823 | Alzheimer's disease | 23 |
| PGS000876 | Alzheimer's disease | 31 |
| PGS000898 | Alzheimer's disease | 40 |
| PGS000945 | Dementia (Alzheimer's, time-to-event) | 26 |
| PGS000969 | Sitting height | 36,345 |
| PGS001229 | Standing height | 51,209 |
| PGS001232 | Fluid intelligence | 10,055 |
| PGS001233 | Heart rate | 14,455 |
| PGS001248 | Gout | 880 |
| PGS001249 | Gout (time-to-event) | 1,796 |
| PGS001287 | Psoriatic arthropathy | 36 |
| PGS001339 | Atrial fibrillation (time-to-event) | 2,142 |
| PGS001340 | Atrial fibrillation | 2,955 |
| PGS001405 | Height | 3,166 |
| PGS001789 | Gout | 910,151 |
| PGS001822 | Gout | 216 |
| PGS001931 | Household income (proxy) | 41,836 |
| PGS002005 | Sitting height | 118,423 |
| PGS002030 | Gout | 163,210 |
| PGS002224 | Sitting height | 910,252 |
| PGS002307 | Gout | 33 |
| PGS002762 | Gout | 1,092,214 |
| PGS002790 | Autism spectrum disorder | 916,713 |
| PGS002849 | BMI | 1,113,832 |
| PGS002851 | BMI | 32,697 |
| PGS003753 | ADHD | 35,445 |
| PGS003896 | Sitting height | 44,543 |
| PGS004304 | Colorectal cancer | 700 |
| PGS004521 | Anxiety disorders | 1,059,939 |
| PGS005393 | PTSD | 53,705 |

**49 cached scores** covering: Alzheimer's, atrial fibrillation, cancer (basal cell, colorectal), gout, height, BMI, autism, ADHD, anxiety, PTSD, psoriatic arthritis, intelligence, heart rate.

## File Format

- Harmonized: `{PGS_ID}_hmPOS_{build}.txt.gz`
- FTP: `https://ftp.ebi.ac.uk/pub/databases/spot/pgs/scores/{PGS_ID}/ScoringFiles/Harmonized/`
- Headers start with `#`, columns: chr_name, chr_position, effect_allele, effect_weight
- Some use GenoBoost dosage format (dosage_0/1/2_weight)

## Regenerate Inventory

```bash
for dir in /data/pgs_cache/PGS*/; do
  id=$(basename "$dir")
  for f in "$dir"*.txt.gz; do
    trait=$(zcat "$f" 2>/dev/null | head -20 | grep "#trait_reported=" | sed 's/#trait_reported=//')
    variants=$(zcat "$f" 2>/dev/null | head -20 | grep "#variants_number=" | sed 's/#variants_number=//')
    echo "$id | $trait | $variants"
    break
  done
done
```
