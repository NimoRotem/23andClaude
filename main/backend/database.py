"""Database setup — SQLAlchemy engine, session, and base."""

import logging

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from backend.config import DATABASE_URL

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """FastAPI dependency: yields a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables."""
    Base.metadata.create_all(bind=engine)


def _add_column_if_missing(conn, table: str, column: str, ddl: str) -> bool:
    """Add a column to an existing SQLite/Postgres table if it does not exist.

    Returns True if a column was added, False if it already existed.
    """
    inspector = inspect(conn)
    try:
        existing_cols = {c["name"] for c in inspector.get_columns(table)}
    except Exception:
        # Table does not exist yet; create_all will handle it
        return False
    if column in existing_cols:
        return False
    conn.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    logger.info("Added column %s.%s", table, column)
    return True


def migrate_user_columns_and_default_user():
    """One-time migration to support multi-user mode.

    1. Adds user_id columns to genomic_files / sample_ancestry / ancestry_pgs_results
       if they do not exist (SQLite needs an explicit ALTER TABLE since
       Base.metadata.create_all only creates new tables, not new columns).
    2. Migrates legacy ScoringRun.user_id == 'default' rows to point at the
       actual admin User.id.
    3. Backfills VCF.created_by_user_id with admin.id where NULL so the existing
       admin can still see / manage them.

    Idempotent: safe to call on every startup.
    """
    from backend.models.schemas import User, ScoringRun, VCF, GenomicFile

    with engine.begin() as conn:
        _add_column_if_missing(conn, "genomic_files", "user_id", "VARCHAR")
        _add_column_if_missing(conn, "sample_ancestry", "user_id", "VARCHAR")
        _add_column_if_missing(conn, "ancestry_pgs_results", "user_id", "VARCHAR")

    db = SessionLocal()
    try:
        admin = db.query(User).filter(User.role == "admin").order_by(User.created_at.asc()).first()
        if admin is None:
            # ensure_default_admin runs before this; should never happen
            return

        # Migrate legacy "default" placeholder rows
        legacy = db.query(ScoringRun).filter(ScoringRun.user_id == "default").all()
        for run in legacy:
            run.user_id = admin.id

        # Backfill VCF ownership
        vcfs_unowned = db.query(VCF).filter(VCF.created_by_user_id.is_(None)).all()
        for vcf in vcfs_unowned:
            vcf.created_by_user_id = admin.id

        # Backfill GenomicFile ownership
        gf_unowned = db.query(GenomicFile).filter(GenomicFile.user_id.is_(None)).all()
        for gf in gf_unowned:
            gf.user_id = admin.id

        if legacy or vcfs_unowned or gf_unowned:
            db.commit()
            logger.info(
                "Multi-user migration: %d runs, %d vcfs, %d genomic_files reassigned to admin",
                len(legacy), len(vcfs_unowned), len(gf_unowned),
            )
    finally:
        db.close()
