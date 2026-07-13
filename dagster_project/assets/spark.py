"""
Spark jobs as Dagster assets.

The asset itself is thin : it invokes the Spark job via subprocess.
Spark sessions and worker processes are managed by Spark itself.
"""

import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent


@asset(
    name="etf_holdings_history_silver",
    description=(
        "Enriched ARK ETF holdings history (silver layer). "
        "Computed via PySpark from the synthetic bronze dataset, "
        "adds holding_rank, weight_change_pct, is_new_holding, "
        "is_exited, days_since_first_appearance."
    ),
    group_name="silver",
    compute_kind="spark",
    deps=["etf_holdings_bronze"],  # depends on Python bronze asset (logical)
)
def etf_holdings_history_silver(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """
    Materialize the silver layer by invoking the Spark job in a subprocess.

    Why subprocess and not direct call ?
      - Spark sessions are heavy : we want a clean process per job
      - Spark logs go to stderr ; subprocess captures them cleanly
      - This pattern works regardless of Spark version
    """
    cmd = [sys.executable, "-m", "spark_jobs.etf_holdings_history"]
    context.log.info(f"Launching Spark job : {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,  # we handle exit code ourselves below
    )

    if result.stdout:
        context.log.info("[Spark stdout]")
        for line in result.stdout.splitlines():
            context.log.info(line)
    if result.stderr:
        context.log.warning("[Spark stderr]")
        for line in result.stderr.splitlines():
            context.log.warning(line)

    if result.returncode != 0:
        raise RuntimeError(f"Spark job failed with exit code {result.returncode}. See logs above.")

    return MaterializeResult(
        metadata={
            "spark_job": "spark_jobs.etf_holdings_history",
            "exit_code": result.returncode,
            "output_path": MetadataValue.text("s3://silver/etf_holdings_history/"),
        }
    )
