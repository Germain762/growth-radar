"""Test orchestrator dispatching across issuers."""

from ingestion.sources.etf_holdings.orchestrator import (
    fetch_etf_holdings,
    find_fetcher,
)


def main():
    # Verify dispatcher routes correctly
    print("Dispatch test :")
    for ticker in ["ARKK", "SOXX", "ICLN", "ARKG", "BOTZ"]:
        fetcher = find_fetcher(ticker)
        if fetcher:
            print(f"  ✅ {ticker} → {fetcher.issuer_name}")
        else:
            print(f"  ❌ {ticker} → no fetcher (will be added later)")

    # Fetch one of each issuer
    print("\nFetching tests :")
    for ticker in ["ARKK", "SOXX"]:
        print(f"\n--- {ticker} ---")
        rows = fetch_etf_holdings(ticker)
        print(f"  Got {len(rows)} validated rows")
        if rows:
            print(f"  Sample : {rows[0]}")


if __name__ == "__main__":
    main()
