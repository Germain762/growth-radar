"""Smoke test : fetch holdings for all 3 iShares funds and validate."""

from ingestion.schemas import EtfHolding
from ingestion.sources.etf_holdings.ishares import IsharesHoldingsFetcher


def test_one(fetcher: IsharesHoldingsFetcher, etf_ticker: str) -> bool:
    print(f"\n{'=' * 60}\nTesting {etf_ticker}\n{'=' * 60}")
    try:
        raw_rows = fetcher.fetch(etf_ticker)
    except Exception as e:
        print(f"❌ FETCH FAILED : {e}")
        return False

    print(f"✅ Fetched {len(raw_rows)} raw rows")
    if not raw_rows:
        print("⚠️  No rows returned")
        return False

    print("\nFirst 3 rows :")
    for row in raw_rows[:3]:
        print(
            f"  {row['company_ticker']:6s} | "
            f"weight={row['weight_pct']:6.3f}% | "
            f"{row.get('company_name', 'N/A')[:40]}"
        )

    valid = []
    failed = []
    for row in raw_rows:
        try:
            valid.append(EtfHolding.model_validate(row))
        except Exception as e:
            failed.append((row.get("company_ticker"), str(e)))

    print(f"\n✅ Valid : {len(valid)}")
    if failed:
        print(f"❌ Failed : {len(failed)}")
        for t, e in failed[:3]:
            print(f"  - {t} : {e[:80]}")
        return False

    total = sum(h.weight_pct for h in valid)
    print(f"📊 Sum of weights : {total:.2f}%")
    return True


def main():
    fetcher = IsharesHoldingsFetcher()
    results = {t: test_one(fetcher, t) for t in ["SOXX", "ICLN", "ITA"]}
    print(f"\n{'=' * 60}\nSUMMARY\n{'=' * 60}")
    for t, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {t}")
    if not all(results.values()):
        exit(1)


if __name__ == "__main__":
    main()
