# 005 — ETF holdings source fragility : scraping vs third-party API

**Status** : Accepted
**Date** : 2026-07
**Deciders** : Germain L

## Context

ETF holdings are scraped directly from issuer websites (iShares, ARK,
and planned : VanEck, Global X, First Trust, KraneShares). Over a few
months of the project, these sources have broken repeatedly :

  - ARK migrated its CSV hosting from ark-funds.com/wp-content to
    assets.ark-funds.com (URL pattern change).
  - iShares retired its `.ajax?fileType=csv` endpoint (now serves the
    product HTML page) in favor of a new `blackrock.com/varnish-api`
    endpoint with different parameters.

Each break produces a cryptic downstream failure (e.g. KeyError on a
missing column) rather than a clear "source changed" signal, and each
requires manual DevTools investigation to find the new endpoint.

## Decision

For Phase 1, keep scraping but harden it and isolate the fragility.
Do NOT yet migrate to a third-party API. Revisit for Phase 2.

## Rationale

### Why keep scraping now

  - It works today and costs nothing (budget ~50€).
  - Scraping heterogeneous issuer formats is itself a learning goal
    (Strategy pattern, per-issuer parsing, resilience).
  - ARK scraping additionally provides multi-year history, which is the
    dataset the Spark job depends on. No free API offers ARK history
    as conveniently.

### Why NOT migrate to an API yet

  - Free ETF-holdings APIs (Financial Modeling Prep free tier,
    yfinance funds_data) have their own limits : rate caps, partial
    coverage, and — for yfinance — only top holdings, not full weighted
    composition.
  - Migrating now, mid-Phase-1, would stall momentum on the modeling
    work (bridge table, marts) that is the actual next milestone.

## Consequences

### Positive
  - No new dependency or cost.
  - Learning value of scraping preserved.

### Negative
  - Sources will break again without warning. Expect periodic maintenance.
  - Each issuer is a separate point of failure.

## Mitigations (implemented)

  - Browser-like headers on requests (User-Agent, Accept).
  - Explicit guard : if the response body is HTML, raise a clear error
    ("endpoint likely changed, re-check via DevTools") instead of
    letting a KeyError surface downstream.
  - Read the composition date from the file itself, with a logged
    fallback, so a preamble change is visible rather than silent.
  - Weekly slow contract test acts as a canary : it must validate that
    the body is parsable CSV with expected columns, NOT merely that the
    HTTP status is 200 (a 200 returning HTML must fail the test).
  - Per-issuer URL discovery documented in each fetcher's docstring.

## Trigger for revisiting (Phase 2)

Migrate iShares/VanEck/GlobalX/FirstTrust/KraneShares to a stable
third-party API if EITHER :
  - maintenance exceeds ~1 fix per issuer per month, OR
  - the project needs guaranteed daily freshness (e.g. live dashboard).

Keep ARK on scraping regardless, for its historical archive.
