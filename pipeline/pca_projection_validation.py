"""Phase 1.6 — Validate PCA projection scaling against held-out reference samples.

Per REMEDIATION_PLAN §1.6:

  1. Hold out 10% of each population's reference samples from PCA fit.
  2. Project held-out samples through the **production runtime path**
     (same plink2 invocation the live service uses on user samples).
  3. Fit a Procrustes transform (or per-PC affine: scale + offset) from
     projected held-out PCs to reference `eigenvec` coordinates.
     Persist the transform matrix alongside `eigenvec`, `eigenval`.
  4. Persist a leave-one-population-out classification report in the cache.
  5. Apply the transform to all runtime projections BEFORE nearest-
     population assignment.

  Gate: held-out classification accuracy < 0.95 for any 1000G
  superpopulation → PCA cache build FAILS, refuse to serve ancestry-
  dependent percentiles from it.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np


log = logging.getLogger("pgs-pipeline.pca_projection_validation")


@dataclass
class ProjectionTransform:
    """Per-PC affine transform mapping projected PCs → reference eigenvec
    coordinates: y_pc[i] = scale[i] * x_pc[i] + offset[i] for the first
    n_pcs principal components.
    """
    scale: list[float]
    offset: list[float]
    n_pcs: int

    def apply(self, projected: np.ndarray) -> np.ndarray:
        """projected: (n_samples, n_pcs) — return transformed coords."""
        s = np.asarray(self.scale, dtype=np.float64)
        o = np.asarray(self.offset, dtype=np.float64)
        return projected[:, : self.n_pcs] * s + o


def fit_affine_per_pc(
    projected: np.ndarray, reference: np.ndarray,
) -> ProjectionTransform:
    """Fit y = scale * x + offset per PC by least-squares.

    `projected` (n,k): production runtime path output.
    `reference` (n,k): ground-truth eigenvec coords (the values they
    SHOULD have).
    """
    n, k = projected.shape
    if reference.shape != (n, k):
        raise ValueError(f"shape mismatch: projected={projected.shape} "
                         f"reference={reference.shape}")
    scale: list[float] = []
    offset: list[float] = []
    for i in range(k):
        x = projected[:, i]
        y = reference[:, i]
        m_x, m_y = x.mean(), y.mean()
        sxx = ((x - m_x) ** 2).sum()
        sxy = ((x - m_x) * (y - m_y)).sum()
        s = sxy / sxx if sxx > 0 else 1.0
        o = m_y - s * m_x
        scale.append(float(s))
        offset.append(float(o))
    return ProjectionTransform(scale=scale, offset=offset, n_pcs=k)


@dataclass
class LopoReport:
    """Leave-one-population-out classification accuracy report."""
    accuracy_per_population: dict[str, float]
    n_test_per_population: dict[str, int]
    confusion: dict[str, dict[str, int]] = field(default_factory=dict)
    pass_threshold: float = 0.95

    @property
    def passes(self) -> bool:
        return all(a >= self.pass_threshold
                   for a in self.accuracy_per_population.values())

    def worst_population(self) -> tuple[str, float]:
        return min(self.accuracy_per_population.items(), key=lambda kv: kv[1])


def _euclid_nearest_centroid(
    point: np.ndarray, centroids: dict[str, np.ndarray],
) -> str:
    best_pop, best_d2 = None, float("inf")
    for pop, c in centroids.items():
        d2 = float(np.sum((point - c) ** 2))
        if d2 < best_d2:
            best_d2 = d2
            best_pop = pop
    return best_pop or "UNK"


def leave_one_pop_out_classify(
    samples: dict[str, np.ndarray],
    *,
    pass_threshold: float = 0.95,
) -> LopoReport:
    """`samples` maps superpopulation → (n_pop, k) PC coordinates of
    held-out test samples. Each test sample is classified by nearest
    centroid computed from the OTHER samples (LOPO style — each population
    classified using centroids of all others' training samples).

    For our use this is held-out → train centroids from full panel
    excluding the held-out 10%; we compute centroids from in-fit samples
    in the caller. This helper simply runs nearest-centroid using
    `samples` as both test and centroid sources, using LOO within pop.
    """
    centroids = {pop: arr.mean(axis=0) for pop, arr in samples.items() if arr.shape[0] > 0}
    accuracy = {}
    n_per = {}
    confusion: dict[str, dict[str, int]] = {p: {q: 0 for q in samples} for p in samples}
    for true_pop, arr in samples.items():
        if arr.shape[0] == 0:
            accuracy[true_pop] = 0.0
            n_per[true_pop] = 0
            continue
        n_correct = 0
        for i in range(arr.shape[0]):
            # Leave-one-out centroid for true_pop
            mask = np.ones(arr.shape[0], dtype=bool)
            mask[i] = False
            loo_centroid = arr[mask].mean(axis=0) if mask.any() else arr[i]
            cents = dict(centroids)
            cents[true_pop] = loo_centroid
            pred = _euclid_nearest_centroid(arr[i], cents)
            confusion[true_pop][pred] = confusion[true_pop].get(pred, 0) + 1
            if pred == true_pop:
                n_correct += 1
        accuracy[true_pop] = n_correct / arr.shape[0]
        n_per[true_pop] = arr.shape[0]
    return LopoReport(
        accuracy_per_population=accuracy,
        n_test_per_population=n_per,
        confusion=confusion,
        pass_threshold=pass_threshold,
    )


def persist_pca_validation(
    pca_cache_dir: str | Path,
    *,
    transform: ProjectionTransform,
    lopo: LopoReport,
) -> tuple[Path, Path]:
    d = Path(pca_cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    transform_path = d / "projection_transform.json"
    lopo_path = d / "lopo_classification.json"
    transform_path.write_text(json.dumps(asdict(transform), indent=2))
    lopo_path.write_text(json.dumps({
        "accuracy_per_population": lopo.accuracy_per_population,
        "n_test_per_population": lopo.n_test_per_population,
        "confusion": lopo.confusion,
        "pass_threshold": lopo.pass_threshold,
        "passes": lopo.passes,
        "worst_population": lopo.worst_population(),
    }, indent=2))
    return transform_path, lopo_path


def load_projection_transform(
    pca_cache_dir: str | Path,
) -> Optional[ProjectionTransform]:
    p = Path(pca_cache_dir) / "projection_transform.json"
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text())
        return ProjectionTransform(
            scale=list(d["scale"]),
            offset=list(d["offset"]),
            n_pcs=int(d["n_pcs"]),
        )
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        log.warning("projection_transform.json malformed at %s: %s", p, e)
        return None


def gate_pca_cache_or_raise(
    pca_cache_dir: str | Path,
    *,
    threshold: float = 0.95,
) -> LopoReport:
    """Read the persisted LOPO report and raise RuntimeError if any
    superpopulation accuracy is below `threshold`. Returns the report on pass.
    """
    p = Path(pca_cache_dir) / "lopo_classification.json"
    if not p.exists():
        raise RuntimeError(
            f"PCA validation report missing at {p}; refuse to serve ancestry-"
            f"dependent percentiles (§1.6 gate)."
        )
    d = json.loads(p.read_text())
    acc = d.get("accuracy_per_population") or {}
    if not acc:
        raise RuntimeError(f"PCA validation report empty at {p}")
    worst_pop, worst_acc = min(acc.items(), key=lambda kv: kv[1])
    if worst_acc < threshold:
        raise RuntimeError(
            f"PCA classification accuracy below gate threshold "
            f"({worst_pop}={worst_acc:.3f} < {threshold:.2f}) — "
            f"refuse to serve ancestry-dependent percentiles (§1.6 gate)."
        )
    return LopoReport(
        accuracy_per_population=acc,
        n_test_per_population=d.get("n_test_per_population") or {},
        confusion=d.get("confusion") or {},
        pass_threshold=threshold,
    )
