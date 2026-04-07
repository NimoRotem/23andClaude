"""SQLAlchemy ORM models for all database tables."""

import datetime
import uuid

from sqlalchemy import (
    Boolean, Column, DateTime, Enum, Float, ForeignKey, Integer, JSON, String, Text,
    func,
)
from sqlalchemy.orm import relationship

from backend.database import Base


def gen_uuid():
    return str(uuid.uuid4())[:12]


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=gen_uuid)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    role = Column(String, default="user")  # admin | user
    created_at = Column(DateTime, default=func.now())
    last_login = Column(DateTime, nullable=True)

    runs = relationship("ScoringRun", back_populates="user")


class VCF(Base):
    __tablename__ = "vcfs"

    id = Column(String, primary_key=True, default=gen_uuid)
    filename = Column(String, nullable=False)
    path_persistent = Column(String, nullable=True)
    path_fast = Column(String, nullable=True)
    genome_build = Column(String, nullable=False)  # GRCh37 | GRCh38
    reference_fasta_path = Column(String, nullable=True)
    reference_fasta_md5 = Column(String, nullable=True)
    samples = Column(JSON, default=list)  # list of sample names
    sample_count = Column(Integer, default=0)
    variant_count = Column(Integer, default=0)
    snp_count = Column(Integer, default=0)
    indel_count = Column(Integer, default=0)
    titv_ratio = Column(Float, nullable=True)
    caller = Column(String, nullable=True)
    caller_version = Column(String, nullable=True)
    qc_status = Column(String, default="pending")  # passed | issues | pending | failed
    qc_checks = Column(JSON, default=dict)
    file_size_bytes = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    created_by_user_id = Column(String, ForeignKey("users.id"), nullable=True)

    runs = relationship("ScoringRun", back_populates="vcf")


class PGSCacheEntry(Base):
    __tablename__ = "pgs_cache"

    pgs_id = Column(String, primary_key=True)
    trait_reported = Column(String, nullable=True)
    trait_efo = Column(JSON, default=list)
    variants_number = Column(Integer, default=0)
    weight_type = Column(String, nullable=True)
    publication_info = Column(JSON, default=dict)
    ancestry_gwas = Column(JSON, default=dict)
    ancestry_eval = Column(JSON, default=dict)
    method_name = Column(String, nullable=True)
    catalog_release_date = Column(String, nullable=True)
    builds_available = Column(JSON, default=list)  # ["GRCh37", "GRCh38"]
    file_path_grch37 = Column(String, nullable=True)
    file_path_grch38 = Column(String, nullable=True)
    metadata_json_path = Column(String, nullable=True)
    downloaded_at = Column(DateTime, nullable=True)
    file_size_bytes = Column(Integer, default=0)


class ScoringRun(Base):
    __tablename__ = "scoring_runs"

    id = Column(String, primary_key=True, default=gen_uuid)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    vcf_id = Column(String, ForeignKey("vcfs.id"), nullable=True)  # nullable for BAM-only runs
    pgs_ids = Column(JSON, default=list)
    engine = Column(String, default="pgsc_calc")  # pgsc_calc | custom | plink2 | auto
    genome_build = Column(String, nullable=False)
    status = Column(String, default="created")  # created|downloading|scoring|ancestry|complete|failed
    progress_pct = Column(Float, default=0.0)
    current_step = Column(String, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration_sec = Column(Float, nullable=True)
    results_path_persistent = Column(String, nullable=True)
    results_path_fast = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    config_snapshot = Column(JSON, default=dict)
    source_files = Column(JSON, default=list)  # [{type, path, vcf_id}, ...]

    user = relationship("User", back_populates="runs")
    vcf = relationship("VCF", back_populates="runs")
    results = relationship("RunResult", back_populates="run", cascade="all, delete-orphan")


class RunResult(Base):
    __tablename__ = "run_results"

    id = Column(String, primary_key=True, default=gen_uuid)
    run_id = Column(String, ForeignKey("scoring_runs.id"), nullable=False)
    pgs_id = Column(String, nullable=False)
    trait = Column(String, nullable=True)
    source_file_path = Column(String, nullable=True)  # path of the source file that produced this result
    source_file_type = Column(String, nullable=True)  # vcf | gvcf | bam
    variants_matched = Column(Integer, default=0)
    variants_total = Column(Integer, default=0)
    match_rate = Column(Float, default=0.0)
    scores_json = Column(JSON, default=list)  # [{sample, raw_score, z_score, rank}]
    created_at = Column(DateTime, default=func.now())

    run = relationship("ScoringRun", back_populates="results")


class GenomicFile(Base):
    __tablename__ = "genomic_files"

    id = Column(String, primary_key=True, default=gen_uuid)
    file_type = Column(String, nullable=False)  # bam, fastq, vcf, gvcf
    path = Column(String, nullable=False, unique=True)
    sample_name = Column(String, nullable=True)
    genome_build = Column(String, nullable=True)  # GRCh37, GRCh38
    file_size_bytes = Column(Integer, default=0)
    format_details = Column(JSON, default=dict)  # paired_path for FASTQ, index_path for BAM, etc.
    vcf_id = Column(String, ForeignKey("vcfs.id"), nullable=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=True, index=True)
    registered_at = Column(DateTime, default=func.now())


class SampleAncestry(Base):
    __tablename__ = "sample_ancestry"

    sample_id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey("users.id"), nullable=True, index=True)
    eur_proportion = Column(Float, default=0.0)
    eas_proportion = Column(Float, default=0.0)
    afr_proportion = Column(Float, default=0.0)
    sas_proportion = Column(Float, default=0.0)
    amr_proportion = Column(Float, default=0.0)
    primary_ancestry = Column(String, nullable=False)
    is_admixed = Column(Boolean, default=False)
    admixture_description = Column(String, nullable=True)
    pc1 = Column(Float, nullable=True)
    pc2 = Column(Float, nullable=True)
    pc3 = Column(Float, nullable=True)
    pc4 = Column(Float, nullable=True)
    pc5 = Column(Float, nullable=True)
    pc6 = Column(Float, nullable=True)
    pc7 = Column(Float, nullable=True)
    pc8 = Column(Float, nullable=True)
    pc9 = Column(Float, nullable=True)
    pc10 = Column(Float, nullable=True)
    inference_method = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now())


class AncestryPGSResult(Base):
    __tablename__ = "ancestry_pgs_results"

    id = Column(String, primary_key=True, default=gen_uuid)
    sample_id = Column(String, ForeignKey("sample_ancestry.sample_id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=True, index=True)
    trait = Column(String, nullable=False)
    pgs_id = Column(String, nullable=False)
    scoring_method = Column(String, nullable=False)
    raw_score = Column(Float, nullable=True)
    combined_score = Column(Float, nullable=True)
    eur_component = Column(Float, nullable=True)
    eas_component = Column(Float, nullable=True)
    percentile = Column(Float, nullable=True)
    reference_population = Column(String, nullable=True)
    reference_n = Column(Integer, nullable=True)
    confidence = Column(String, nullable=True)
    covered_fraction = Column(Float, nullable=True)
    ancestry_warnings = Column(JSON, default=list)
    pgs_training_pop = Column(String, nullable=True)
    pgs_training_pop_match = Column(Boolean, nullable=True)
    created_at = Column(DateTime, default=func.now())
