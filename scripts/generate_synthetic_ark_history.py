"""
Generate a synthetic historical dataset for ARK ETF holdings.

PURPOSE :
  Provide a realistic-looking dataset (~50-200k rows) on which to
  develop and test the Spark backfill job. The data is NOT real —
  weights are randomly perturbed daily snapshots of today's holdings.

OUTPUT :
  s3://bronze/etf_holdings_synthetic/etf_ticker=XXX/composition_date=YYYY-MM-DD/part.parquet

WARNING :
  Do NOT use this for actual investment analysis. It exists only
  to provide a non-trivial volume for Spark practice.
"""

import io
import random
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.common.s3_client import get_s3_client
from ingestion.sources.etf_holdings.ark import ArkHoldingsFetcher

BRONZE_BUCKET = "bronze"
SYNTHETIC_PREFIX = "etf_holdings_synthetic"

# Generate 3 years of business-day history (~750 days × 3 ETFs × 35 holdings ≈ 80k rows)
START_DATE = date(2022, 1, 3)
END_DATE = date.today()
PERTURBATION_PCT = 0.05  # ±5% random walk per day on weights
RANDOM_SEED = 42


def business_days(start: date, end: date) -> list[date]:
    """Return all business days (Mon-Fri) between start and end inclusive."""
    out = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            out.append(current)
        current += timedelta(days=1)
    return out


def perturb_weights(base_holdings: list[dict], rng: random.Random) -> list[dict]:
    """
    Apply random perturbations to weights, then renormalize to sum to ~100%.

    This simulates daily holding rebalancing :
      - Each weight drifts by +/- PERTURBATION_PCT
      - Sum is renormalized so weights still add up to 100%
    """
    perturbed = []
    for h in base_holdings:
        delta = rng.uniform(-PERTURBATION_PCT, PERTURBATION_PCT)
        new_weight = h["weight_pct"] * (1 + delta)
        perturbed.append({**h, "weight_pct": max(0.01, new_weight)})

    # Renormalize so the total is back to 100%
    total = sum(h["weight_pct"] for h in perturbed)
    factor = 100.0 / total
    for h in perturbed:
        h["weight_pct"] = round(h["weight_pct"] * factor, 4)

    return perturbed


def write_partition(
    rows: list[dict],
    etf_ticker: str,
    composition_date: date,
    s3_client,
) -> None:
    """Write one (etf, date) partition as Parquet to MinIO."""
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    key = (
        f"{SYNTHETIC_PREFIX}"
        f"/etf_ticker={etf_ticker.upper()}"
        f"/composition_date={composition_date.isoformat()}"
        f"/part.parquet"
    )
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=buf.getvalue(),
    )


def main():
    rng = random.Random(RANDOM_SEED)
    s3 = get_s3_client()

    fetcher = ArkHoldingsFetcher()
    days = business_days(START_DATE, END_DATE)
    print(f"Generating {len(days)} business days × 3 ETFs = {len(days) * 3} files")

    for etf_ticker in ["ARKK", "ARKQ", "ARKG"]:
        print(f"\n=== {etf_ticker} ===")

        # Fetch current holdings as the base distribution
        base_holdings = fetcher.fetch(etf_ticker, composition_date=END_DATE)
        if not base_holdings:
            print(f"  ⚠️  No base holdings for {etf_ticker}, skipping")
            continue

        print(f"  Base holdings : {len(base_holdings)} positions")
        print(f"  Generating {len(days)} historical snapshots...")

        for i, d in enumerate(days):
            perturbed = perturb_weights(base_holdings, rng)

            # Adjust composition_date and fetched_at for each historical snapshot
            for h in perturbed:
                h["composition_date"] = d
                h["fetched_at"] = datetime.combine(d, datetime.min.time(), UTC)

            write_partition(perturbed, etf_ticker, d, s3)

            # Progress every 100 days
            if (i + 1) % 100 == 0:
                print(f"  ... {i + 1}/{len(days)} days written")

        print(f"  ✅ {etf_ticker} : {len(days)} snapshots written")

    print("\n✅ Synthetic history generation complete")
    total_files = len(days) * 3
    total_rows = total_files * 35  # rough estimate
    print(f"   Approximate total : {total_files} files, ~{total_rows} rows")


if __name__ == "__main__":
    main()
