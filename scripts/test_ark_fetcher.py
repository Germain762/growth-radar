"""Smoke test : fetch holdings for all 3 ARK funds and validate via Pydantic."""

from ingestion.schemas import EtfHolding
from ingestion.sources.etf_holdings.ark import ArkHoldingsFetcher


def test_one(fetcher: ArkHoldingsFetcher, etf_ticker: str) -> bool:
    """Test one ARK ETF. Returns True on success."""
    print(f"\n{'=' * 60}")
    print(f"Testing {etf_ticker}")
    print(f"{'=' * 60}")

    try:
        raw_rows = fetcher.fetch(etf_ticker)
    except Exception as e:
        print(f"❌ FETCH FAILED : {e}")
        return False

    print(f"✅ Fetched {len(raw_rows)} raw rows")

    if not raw_rows:
        print("⚠️  No rows returned — check the URL is still valid")
        return False

    # Show a sample
    print("\nFirst 3 rows :")
    for row in raw_rows[:3]:
        print(
            f"  {row['company_ticker']:6s} | "
            f"weight={row['weight_pct']:6.2f}% | "
            f"{row.get('company_name', 'N/A')[:40]}"
        )

    # Validate all rows via Pydantic
    valid = []
    failed = []
    for row in raw_rows:
        try:
            valid.append(EtfHolding.model_validate(row))
        except Exception as e:
            failed.append((row.get("company_ticker"), str(e)))

    print(f"\n✅ Valid via Pydantic : {len(valid)}")
    if failed:
        print(f"❌ Failed : {len(failed)}")
        for ticker, err in failed[:3]:
            print(f"  - {ticker} : {err[:80]}")
        return False

    # Sanity check : weights should approximately sum to 100%
    total_weight = sum(h.weight_pct for h in valid)
    print(f"📊 Sum of weights : {total_weight:.2f}%")
    if abs(total_weight - 100) > 5:
        print("⚠️  Sum is far from 100% — might be missing rows or wrong scaling")

    return True


def main():
    fetcher = ArkHoldingsFetcher()
    results = {}
    for ticker in ["ARKK", "ARKQ", "ARKG"]:
        results[ticker] = test_one(fetcher, ticker)

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    for ticker, success in results.items():
        status = "✅" if success else "❌"
        print(f"  {status} {ticker}")

    if not all(results.values()):
        exit(1)


if __name__ == "__main__":
    main()
