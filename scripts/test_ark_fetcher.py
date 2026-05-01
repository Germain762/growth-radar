"""Quick smoke test : fetch ARKK holdings and print first few rows."""

from ingestion.schemas import EtfHolding
from ingestion.sources.etf_holdings.ark import ArkHoldingsFetcher


def main():
    fetcher = ArkHoldingsFetcher()
    raw_rows = fetcher.fetch("ARKK")

    print(f"\n✅ Fetched {len(raw_rows)} raw rows from ARKK")
    print("\nFirst 3 raw rows :")
    for row in raw_rows[:3]:
        print(f"  {row}")

    # Validate via Pydantic
    print("\nValidating via EtfHolding schema...")
    valid = []
    failed = []
    for row in raw_rows:
        try:
            valid.append(EtfHolding.model_validate(row))
        except Exception as e:
            failed.append((row.get("company_ticker"), str(e)))

    print(f"\n✅ Valid : {len(valid)}")
    print(f"❌ Failed : {len(failed)}")
    if failed:
        print("Failures :")
        for ticker, err in failed[:5]:
            print(f"  - {ticker} : {err}")


if __name__ == "__main__":
    main()
