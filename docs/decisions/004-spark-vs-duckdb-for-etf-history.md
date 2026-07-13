# 004 — Spark vs DuckDB for ETF holdings history

**Status** : Accepted
**Date** : 2026-05
**Deciders** : Germain L

## Context

The ETF holdings history dataset (~80 000 rows currently, growing to
maybe ~500 000 in production) needs to be enriched with rank, weight
deltas, new/exited flags. The candidate engines are :
  - DuckDB (already in the stack via dbt-duckdb)
  - PySpark (target learning outcome)

## Decision

We use PySpark for this transformation, despite DuckDB being technically
better suited for the volume.

## Rationale

### Why DuckDB would be objectively faster

  - DuckDB processes 80k rows in ~200ms
  - Spark startup alone takes 5-10 seconds
  - DuckDB's columnar engine is hand-optimized for analytic workloads
  - No JVM overhead, no S3 connector setup

### Why we use Spark anyway

  1. **Pedagogical** : the project's primary goal is to gain hands-on
     PySpark experience (DataFrames, window functions, Spark UI,
     partitioning) on a non-trivial dataset.

  2. **Market value** : Spark remains a high-demand skill on the data
     engineering job market. A portfolio project demonstrating Spark
     familiarity opens more doors than one without.

  3. **Realistic preparation** : when the dataset grows to millions
     of rows (Phase 2 with news embeddings), Spark becomes objectively
     necessary. Learning it now on smaller data builds the foundation.

### Why not just DuckDB now and switch later ?

  - Refactoring from DuckDB to Spark is a significant rewrite (different
    APIs, different idioms). Doing it now while the codebase is small
    is cheaper than later.
  - The "switch trigger" in the future would be subtle — likely we'd
    procrastinate and stay on DuckDB even when it gets slow.

## Consequences

### Positive
  - We gain real PySpark experience on a controlled dataset
  - We can confidently say in interviews : "I chose Spark for these
    transformations after evaluating DuckDB and explicitly trading off
    performance for skill development and future-proofing"
  - The Phase 2 (news NLP) build will be smoother because Spark is
    already integrated

### Negative
  - The job is slower than necessary today (~45s vs ~5s with DuckDB)
  - Initial JVM/JAR setup adds operational complexity
  - Spark errors are harder to debug than DuckDB errors

## Mitigations

  - Document this trade-off transparently (this ADR + README mention)
  - Keep the Spark job thin and idempotent : easy to swap back if needed
  - Define a clear migration trigger : if Phase 2 stays small (<5M rows),
    revisit DuckDB
