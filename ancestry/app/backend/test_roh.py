"""Unit tests for ROH output parsing — prevents regression of bp→Mb conversion bug."""

import os
import tempfile

import pytest

from pipeline import _parse_roh_summary, _parse_roh_hom_file

GENOME_SIZE_MB = 3088  # Human genome size in Mb
GENOME_SIZE_KB = GENOME_SIZE_MB * 1000  # 3,088,000 kb


def _write_summary(tmpdir: str, nseg: int, kb: float, kbavg: float) -> str:
    """Write a mock .hom.summary file and return its path."""
    path = os.path.join(tmpdir, "roh.hom.summary")
    with open(path, "w") as f:
        f.write("FID\tIID\tPHE\tNSEG\tKB\tKBAVG\n")
        f.write(f"SAMPLE\tSAMPLE\t-9\t{nseg}\t{kb}\t{kbavg}\n")
    return path


def _write_hom(tmpdir: str, segments_kb: list) -> str:
    """Write a mock .hom file with segment lengths in kb. Returns path."""
    path = os.path.join(tmpdir, "roh.hom")
    with open(path, "w") as f:
        f.write("FID IID PHE CHR SNP1 SNP2 POS1 POS2 KB NSNP DENSITY PHOM PHET\n")
        for i, kb in enumerate(segments_kb):
            f.write(f"S S -9 1 rs{i} rs{i+1} {i*1000} {i*1000+int(kb*1000)} {kb} 100 50 1 0\n")
    return path


class TestROHSummaryParser:
    """Tests for _parse_roh_summary."""

    def test_normal_values(self):
        """Normal ROH values should parse correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_summary(tmpdir, nseg=15, kb=85000, kbavg=5666.67)
            result = _parse_roh_summary(path)
            assert result is not None
            assert result["total_mb"] == 85.0
            assert result["n_segments"] == 15
            assert result["avg_kb"] == 5666.67

    def test_total_mb_below_genome_size(self):
        """total_mb must be less than human genome size (3,088 Mb)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_summary(tmpdir, nseg=10, kb=50000, kbavg=5000)
            result = _parse_roh_summary(path)
            assert result is not None
            assert result["total_mb"] < GENOME_SIZE_MB, (
                f"total_mb={result['total_mb']} exceeds genome size {GENOME_SIZE_MB} Mb — "
                f"likely a unit mismatch (bp reported as kb?)"
            )

    def test_avg_kb_below_genome_size(self):
        """avg_kb must be less than genome size in kb (3,088,000 kb)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_summary(tmpdir, nseg=5, kb=25000, kbavg=5000)
            result = _parse_roh_summary(path)
            assert result is not None
            assert result["avg_kb"] < GENOME_SIZE_KB, (
                f"avg_kb={result['avg_kb']} exceeds genome size {GENOME_SIZE_KB} kb — "
                f"likely a unit mismatch"
            )

    def test_detects_unit_mismatch_total(self):
        """If total_kb is actually in bp (very large), total_mb would exceed genome size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Simulate bp values reported as kb: 85,000,000 "kb" = 85,000 Mb (clearly wrong)
            path = _write_summary(tmpdir, nseg=10, kb=85000000, kbavg=8500000)
            result = _parse_roh_summary(path)
            assert result is not None
            assert result["total_mb"] > GENOME_SIZE_MB, (
                "Expected this bad data to exceed genome size threshold"
            )

    def test_empty_file(self):
        """Empty summary file should return None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "empty.hom.summary")
            with open(path, "w") as f:
                f.write("")
            assert _parse_roh_summary(path) is None

    def test_bottleneck_detection(self):
        """Bottleneck flag should fire for >50 Mb total ROH."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_summary(tmpdir, nseg=20, kb=75000, kbavg=3750)
            result = _parse_roh_summary(path)
            assert result is not None
            assert result["bottleneck"] is True


class TestROHHomParser:
    """Tests for _parse_roh_hom_file (fallback parser)."""

    def test_normal_segments(self):
        """Parse multiple segments correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_hom(tmpdir, [5000, 3000, 7000, 2000])
            result = _parse_roh_hom_file(path)
            assert result is not None
            assert result["total_mb"] == 17.0
            assert result["n_segments"] == 4
            assert result["total_mb"] < GENOME_SIZE_MB

    def test_total_below_genome(self):
        """Parsed total must be below genome size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_hom(tmpdir, [500, 1200, 800])
            result = _parse_roh_hom_file(path)
            assert result is not None
            assert result["total_mb"] < GENOME_SIZE_MB
            assert result["avg_kb"] < GENOME_SIZE_KB

    def test_missing_file(self):
        """Missing .hom file should return None."""
        assert _parse_roh_hom_file("/nonexistent/path.hom") is None
