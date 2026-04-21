#!/usr/bin/env python3
"""Unit tests for indel vs SNP routing in _pileup_genotype.

Regression guard: ensures len(ref) != len(alt) routes to CIGAR-based
indel genotyper, not the single-base SNP pileup.
"""
import sys
import os
sys.path.insert(0, '/home/nimrod_rotem/simple-genomics')

import unittest
from unittest.mock import patch, MagicMock


class TestIndelRouting(unittest.TestCase):
    """Test that _pileup_genotype correctly dispatches indels vs SNPs."""

    def test_snp_uses_pileup_path(self):
        """SNPs (len(ref) == len(alt)) should use the standard pileup."""
        from runners import _pileup_genotype
        with patch('runners._pileup_genotype_indel') as mock_indel:
            # Mock pysam to avoid needing a BAM file
            with patch('pysam.AlignmentFile'):
                try:
                    _pileup_genotype('/fake.bam', 'chr1', 100, 'T', 'G')
                except:
                    pass
            # Should NOT have called the indel function
            mock_indel.assert_not_called()

    def test_deletion_routes_to_indel(self):
        """Deletions (len(ref) > len(alt)) should route to indel genotyper."""
        from runners import _pileup_genotype
        with patch('runners._pileup_genotype_indel', return_value={
            "found": True, "genotype": "0/0", "source": "pileup_indel"
        }) as mock_indel:
            result = _pileup_genotype('/fake.bam', 'chr7', 117559593, 'ATCT', 'A')
            mock_indel.assert_called_once()
            self.assertEqual(result["source"], "pileup_indel")

    def test_insertion_routes_to_indel(self):
        """Insertions (len(alt) > len(ref)) should route to indel genotyper."""
        from runners import _pileup_genotype
        with patch('runners._pileup_genotype_indel', return_value={
            "found": True, "genotype": "0/0", "source": "pileup_indel"
        }) as mock_indel:
            result = _pileup_genotype('/fake.bam', 'chr15', 72349074, 'G', 'GTATC')
            mock_indel.assert_called_once()
            self.assertEqual(result["source"], "pileup_indel")

    def test_single_base_substitution_not_indel(self):
        """A>G is a SNP, not an indel."""
        from runners import _pileup_genotype
        with patch('runners._pileup_genotype_indel') as mock_indel:
            with patch('pysam.AlignmentFile'):
                try:
                    _pileup_genotype('/fake.bam', 'chr17', 80104568, 'T', 'G')
                except:
                    pass
            mock_indel.assert_not_called()

    def test_cftr_f508del_is_deletion(self):
        """CFTR F508del (ATCT>A) should be classified as 3bp deletion."""
        ref, alt = 'ATCT', 'A'
        self.assertGreater(len(ref), len(alt))
        self.assertEqual(len(ref) - len(alt), 3)  # 3bp deletion

    def test_hexa_insertion_is_insertion(self):
        """HEXA 1278insTATC (G>GTATC) should be classified as 4bp insertion."""
        ref, alt = 'G', 'GTATC'
        self.assertGreater(len(alt), len(ref))
        self.assertEqual(len(alt) - len(ref), 4)  # 4bp insertion


class TestIndelGenotyperOnRealBAM(unittest.TestCase):
    """Integration tests against actual BAM (skip if not available)."""

    BAM = '/data/aligned_bams/Nimo.bam'

    def setUp(self):
        if not os.path.exists(self.BAM):
            self.skipTest(f"BAM not available: {self.BAM}")

    def test_cftr_f508del_negative(self):
        """Nimo should be 0/0 for CFTR F508del (no deletion)."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, 'chr7', 117559593, 'ATCT', 'A')
        self.assertEqual(r['source'], 'pileup_indel')
        self.assertEqual(r['genotype'], '0/0')
        self.assertEqual(r['allele_counts']['alt'], 0)
        self.assertGreater(r['depth'], 10)

    def test_hexa_insertion_negative(self):
        """Nimo should be 0/0 for HEXA 1278insTATC (no insertion)."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, 'chr15', 72349074, 'G', 'GTATC')
        self.assertEqual(r['source'], 'pileup_indel')
        self.assertEqual(r['genotype'], '0/0')
        self.assertEqual(r['allele_counts']['alt'], 0)
        self.assertGreater(r['depth'], 10)

    def test_gaa_snp_standard_path(self):
        """GAA c.-32-13T>G is a SNP — should use standard pileup and find it."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, "17", 80104542, "T", "G")
        self.assertEqual(r['source'], 'pileup')
        self.assertTrue(r['found'])
        # GAA is 0/0 at correct position (no Pompe variant)
        self.assertEqual(r['genotype'], '0/0')




class TestRefSanityGuard(unittest.TestCase):
    """Test that the reference-base sanity check catches wrong positions."""

    BAM = '/data/aligned_bams/Nimo.bam'

    def setUp(self):
        if not os.path.exists(self.BAM):
            self.skipTest(f"BAM not available: {self.BAM}")

    def test_wrong_gaa_position_caught(self):
        """Old wrong GAA position (17:80104568) should trigger sanity check."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, '17', 80104568, 'T', 'G')
        self.assertTrue(r.get('ref_sanity_failed'))
        self.assertEqual(r['dosage'], 0)

    def test_wrong_acadm_position_caught(self):
        """Old wrong ACADM position (1:76190043) should trigger sanity check."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, '1', 76190043, 'A', 'G')
        self.assertTrue(r.get('ref_sanity_failed'))
        self.assertEqual(r['dosage'], 0)

    def test_correct_gaa_position(self):
        """Correct GAA position (17:80104542) should give 0/0 for Nimo."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, '17', 80104542, 'T', 'G')
        self.assertFalse(r.get('ref_sanity_failed'))
        self.assertEqual(r['genotype'], '0/0')
        self.assertEqual(r['dosage'], 0)
        self.assertGreater(r['depth'], 10)

    def test_correct_acadm_position(self):
        """Correct ACADM position (1:75761161) should give 0/0 for Nimo."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, '1', 75761161, 'A', 'G')
        self.assertFalse(r.get('ref_sanity_failed'))
        self.assertEqual(r['genotype'], '0/0')
        self.assertEqual(r['dosage'], 0)
        self.assertGreater(r['depth'], 10)

    def test_strand_counts_present(self):
        """Genotype results should include strand count information."""
        from runners import _pileup_genotype
        r = _pileup_genotype(self.BAM, '17', 80104542, 'T', 'G')
        self.assertIn('strand_counts', r)
        self.assertIn('fwd', r['strand_counts'])
        self.assertIn('rev', r['strand_counts'])

if __name__ == '__main__':
    unittest.main(verbosity=2)
