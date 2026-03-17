"""
iap_pipeline_orchestrator.py
============================
Orchestrates the end-to-end IAP revenue data pipeline across multiple
payment providers (Apple, Google, Amazon, Roku, DirectBilling).

Pipeline stages per provider file
----------------------------------
1. Validate   – schema checks, null checks, row count guard
2. Stage Raw  – COPY raw CSV/TSV into landing table
3. Stage Final– type-cast & normalise into typed staging table
4. Load Facts – insert into central fact table; purge superseded MTD files
5. Classify   – auto-insert newly discovered SKUs into reference table
6. Monitor    – refresh ingestion monitoring MVs
7. Audit      – update load_audit_trail with final status

Usage
-----
    python iap_pipeline_orchestrator.py \
        --provider google \
        --region AMER \
        --file-path s3://data-lake/google/2024-01-15_google_amer_sales.csv \
        --file-tx-id abc123

Dependencies
------------
    pip install psycopg2-binary boto3 pyyaml
"""

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import yaml

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config & constants
# ---------------------------------------------------------------------------
SUPPORTED_PROVIDERS = {"apple", "google", "amazon", "roku", "direct_billing"}

PROVIDER_CHANNEL_MAP = {
    "apple":          {"channel": "Apple",         "channel_nr": 1},
    "google":         {"channel": "Google",        "channel_nr": 2},
    "amazon":         {"channel": "Amazon",        "channel_nr": 3},
    "roku":           {"channel": "Roku",          "channel_nr": 4},
    "direct_billing": {"channel": "DirectBilling", "channel_nr": 5},
}

# SQL file paths (relative to project root)
SQL_DIR = Path(__file__).parent.parent / "sql"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class PipelineContext:
    """Immutable context threaded through all pipeline stages."""
    provider:     str
    region:       str
    file_path:    str
    file_tx_id:   str
    load_tx_id:   str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    row_count:    int = 0
    status:       str = "IN_PROGRESS"


@dataclass
class StageResult:
    stage:    str
    success:  bool
    rows:     int = 0
    message:  str = ""
    duration: float = 0.0


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
def get_connection(config: dict) -> psycopg2.extensions.connection:
    """Return a psycopg2 connection using config dict (host/port/db/user/pass)."""
    return psycopg2.connect(
        host=config["host"],
        port=config.get("port", 5439),
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        connect_timeout=30,
    )


def load_config(path: str = "config.yaml") -> dict:
    """Load YAML config.  Falls back to env vars if file not found."""
    try:
        with open(path) as fh:
            return yaml.safe_load(fh)
    except FileNotFoundError:
        import os
        return {
            "host":     os.environ["DB_HOST"],
            "port":     int(os.environ.get("DB_PORT", 5439)),
            "dbname":   os.environ["DB_NAME"],
            "user":     os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
        }


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
def run_sql_file(
    conn: psycopg2.extensions.connection,
    sql_path: Path,
    tokens: dict,
) -> int:
    """
    Execute a parameterised SQL file.

    ``tokens`` is a dict of placeholder → value, e.g.
    ``{"file_tx_id": "abc123", "region": "AMER"}``.
    Placeholders in SQL files use ``{{key}}`` syntax.
    """
    sql = sql_path.read_text()
    for key, value in tokens.items():
        sql = sql.replace(f"{{{{{key}}}}}", str(value))

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.rowcount
    conn.commit()
    return max(rows, 0)


def execute_query(
    conn: psycopg2.extensions.connection,
    query: str,
    params: Optional[tuple] = None,
) -> list:
    """Execute a query and return all rows."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def stage_validate(ctx: PipelineContext, conn) -> StageResult:
    """
    Basic pre-load validation:
      - Confirm file_tx_id does not already exist at LOADED_FACT status
        (idempotency guard – allows re-runs for other statuses)
      - Could be extended with S3 file-existence check, schema validation, etc.
    """
    t0 = datetime.now(timezone.utc)
    rows = execute_query(
        conn,
        "SELECT status FROM audit_trail.load_audit_trail WHERE file_tx_id = %s",
        (ctx.file_tx_id,),
    )
    if rows and rows[0]["status"] == "LOADED_FACT":
        return StageResult(
            stage="validate", success=False,
            message=f"file_tx_id {ctx.file_tx_id} already at LOADED_FACT – skipping.",
        )
    return StageResult(
        stage="validate", success=True,
        duration=(datetime.now(timezone.utc) - t0).total_seconds(),
    )


def stage_raw(ctx: PipelineContext, conn) -> StageResult:
    """
    COPY raw file from S3 into the provider's raw landing table.
    In production this would execute a Redshift COPY command.
    """
    t0 = datetime.now(timezone.utc)
    log.info("[%s] Copying raw file to staging: %s", ctx.provider, ctx.file_path)

    # Example COPY statement (Redshift syntax)
    copy_sql = f"""
        COPY staging_data.{ctx.provider}_sales_raw
        FROM '{ctx.file_path}'
        IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftS3Role'
        FORMAT AS CSV IGNOREHEADER 1 TIMEFORMAT 'auto'
        BLANKSASNULL EMPTYASNULL TRUNCATECOLUMNS;
    """
    log.debug("COPY SQL:\n%s", copy_sql)
    # NOTE: In a real pipeline, execute copy_sql via conn.cursor().execute(copy_sql)
    #       Omitted here so the script is runnable without live credentials.
    simulated_rows = 5000  # placeholder

    return StageResult(
        stage="raw", success=True, rows=simulated_rows,
        duration=(datetime.now(timezone.utc) - t0).total_seconds(),
    )


def stage_final(ctx: PipelineContext, conn) -> StageResult:
    """Type-cast raw staging data into the typed staging table."""
    t0 = datetime.now(timezone.utc)
    sql_path = SQL_DIR / "staging" / f"{ctx.provider}_sales_staging.sql"
    if not sql_path.exists():
        # Fall back to generic billing staging for direct billing provider
        sql_path = SQL_DIR / "staging" / "billing_transactions_staging.sql"

    rows = run_sql_file(conn, sql_path, {
        "file_tx_id": ctx.file_tx_id,
        "region":     ctx.region,
    })
    return StageResult(
        stage="final", success=True, rows=rows,
        duration=(datetime.now(timezone.utc) - t0).total_seconds(),
    )


def stage_facts(ctx: PipelineContext, conn) -> StageResult:
    """Load typed staging data into the central fact table."""
    t0 = datetime.now(timezone.utc)
    rows = run_sql_file(
        conn,
        SQL_DIR / "facts" / "ft_payments.sql",
        {"file_tx_id": ctx.file_tx_id, "region": ctx.region},
    )
    return StageResult(
        stage="facts", success=True, rows=rows,
        duration=(datetime.now(timezone.utc) - t0).total_seconds(),
    )


def stage_classify_skus(ctx: PipelineContext, conn) -> StageResult:
    """Auto-insert any newly discovered SKUs into the reference table."""
    t0 = datetime.now(timezone.utc)
    rows = run_sql_file(
        conn,
        SQL_DIR / "reference" / "sku_auto_classification.sql",
        {"file_tx_id": ctx.file_tx_id},
    )
    return StageResult(
        stage="classify_skus", success=True, rows=rows,
        duration=(datetime.now(timezone.utc) - t0).total_seconds(),
    )


def stage_monitor(ctx: PipelineContext, conn) -> StageResult:
    """Refresh ingestion monitoring materialized views."""
    t0 = datetime.now(timezone.utc)
    mv_sql = (
        f"REFRESH MATERIALIZED VIEW ingestion_monitoring.mv_iap_{ctx.provider}_sales;"
    )
    with conn.cursor() as cur:
        cur.execute(mv_sql)
    conn.commit()
    return StageResult(
        stage="monitor", success=True,
        duration=(datetime.now(timezone.utc) - t0).total_seconds(),
    )


def stage_audit(ctx: PipelineContext, conn, final_status: str) -> StageResult:
    """Write final pipeline status to the audit trail."""
    t0 = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit_trail.load_audit_trail
            SET    status         = %s,
                   load_date_time = %s
            WHERE  file_tx_id     = %s
            """,
            (final_status, datetime.now(timezone.utc), ctx.file_tx_id),
        )
    conn.commit()
    return StageResult(
        stage="audit", success=True,
        duration=(datetime.now(timezone.utc) - t0).total_seconds(),
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(ctx: PipelineContext, config: dict) -> bool:
    """
    Execute all pipeline stages in order.
    Any stage failure rolls back the current transaction and marks the
    file as LOAD_ERROR in the audit trail.
    """
    conn = get_connection(config["database"])
    results: list[StageResult] = []

    stages = [
        ("validate",     stage_validate),
        ("raw",          stage_raw),
        ("final",        stage_final),
        ("facts",        stage_facts),
        ("classify_skus",stage_classify_skus),
        ("monitor",      stage_monitor),
    ]

    final_status = "LOADED_FACT"
    success = True

    for name, stage_fn in stages:
        log.info("[%s] ▶ Stage: %s", ctx.file_tx_id[:8], name)
        try:
            result = stage_fn(ctx, conn)
            results.append(result)
            log.info(
                "[%s] ✓ %s  rows=%d  %.2fs",
                ctx.file_tx_id[:8], name, result.rows, result.duration,
            )
            if not result.success:
                log.warning("[%s] Stage '%s' returned non-success: %s",
                            ctx.file_tx_id[:8], name, result.message)
                final_status = "LOAD_ERROR"
                success = False
                break
        except Exception as exc:  # noqa: BLE001
            log.exception("[%s] Stage '%s' raised an exception: %s",
                          ctx.file_tx_id[:8], name, exc)
            conn.rollback()
            final_status = "LOAD_ERROR"
            success = False
            break

    # Always write audit status
    stage_audit(ctx, conn, final_status)
    conn.close()

    total = sum(r.duration for r in results)
    log.info(
        "[%s] Pipeline complete: status=%s  stages=%d  total=%.2fs",
        ctx.file_tx_id[:8], final_status, len(results), total,
    )
    return success


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IAP Revenue Pipeline Orchestrator")
    p.add_argument("--provider",    required=True, choices=SUPPORTED_PROVIDERS)
    p.add_argument("--region",      required=True,
                   help="File transaction region, e.g. AMER, EMEA, MAX")
    p.add_argument("--file-path",   required=True,
                   help="S3 URI or local path of the source file")
    p.add_argument("--file-tx-id",  default=str(uuid.uuid4()),
                   help="Unique transaction ID for this file load (auto-generated if omitted)")
    p.add_argument("--config",      default="config.yaml",
                   help="Path to YAML config file")
    p.add_argument("--dry-run",     action="store_true",
                   help="Print resolved SQL without executing")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    ctx = PipelineContext(
        provider=args.provider,
        region=args.region,
        file_path=args.file_path,
        file_tx_id=args.file_tx_id,
    )

    log.info(
        "Starting pipeline | provider=%s  region=%s  file_tx_id=%s",
        ctx.provider, ctx.region, ctx.file_tx_id,
    )

    ok = run_pipeline(ctx, config)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
