# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spark SQL Code Coverage — a Scala tool that measures **expression-level test coverage** for Spark SQL queries. Given `.sql` source files and CSV test data, it identifies coverable expressions (CASE branches, WHERE predicates, JOIN conditions, etc.), evaluates whether test data satisfies each expression, and generates coverage reports.

Package namespace: `com.bob.sparkcoverage`

## Tech Stack

- **Scala 2.13** with **Maven** (scala-maven-plugin)
- **Apache Spark 3.5.x** — CatalystSqlParser for SQL parsing, QueryExecutionListener for runtime engine (provided scope)
- **DuckDB** — embedded SQL engine for the static coverage engine
- **scopt** — CLI argument parsing
- **ScalaTest** — testing framework

## Build Commands

```bash
mvn compile          # Compile
mvn test             # Run all tests
mvn test -Dsuites="com.bob.sparkcoverage.parser.SqlFileParserSpec"  # Run single test class
mvn package          # Build fat JAR (with shade plugin)
```

## Architecture

Two-engine design behind a shared `CoverageEngine` trait:

- **StaticCoverageEngine** — Loads CSVs into DuckDB in-memory, evaluates predicates via SQL. No Spark runtime needed. Fast (~seconds). Best for CI/CD and local dev.
- **CatalystCoverageEngine** — Uses a SparkSession to load CSVs and execute SQL. Can detect optimizer-pruned branches by comparing analyzed vs optimized plans. Full Spark SQL dialect compatibility.

Shared components:
- **ExpressionExtractor** — Walks Catalyst AST (LogicalPlan/Expression trees) to find coverable expressions
- **PredicateExtractor** — Derives testable SQL predicates from each coverable expression
- **SqlFileParser** — Reads `.sql` files, splits on semicolons, tracks line numbers
- **DataSourceConfig** — Maps table names to CSV files via YAML config (`coverage-config.yaml`) with convention-based fallback (`<data-dir>/<tableName>.csv`)
- **LineageTracer** — Column-level data flow tracking through SQL transformations
- **Reporters** — CLI (ANSI-colored), HTML (highlighted SQL spans), JSON (machine-readable for CI/CD)

## Coverage Algorithm

1. Parse `.sql` files into SQL statements
2. Walk Catalyst AST to extract coverable expressions (CASE_BRANCH, CASE_ELSE, WHERE_PREDICATE, JOIN_CONDITION, HAVING_PREDICATE, COALESCE_BRANCH, IF_BRANCH, NULLIF_BRANCH, SUBQUERY_PREDICATE, FILTER_PREDICATE)
3. Derive a testable predicate for each expression (e.g., `CASE WHEN status = 'active'` becomes predicate `status = 'active'`)
4. Evaluate each predicate: `SELECT EXISTS (SELECT 1 FROM <table> WHERE <predicate>)`
5. If any row satisfies the predicate, the expression is covered

## Testing

A feature is not considered done until **all** tests pass (`mvn test`). This includes:

- **Unit tests** — Test individual components in isolation. The agent implementing the feature is expected to write these.
- **Integration tests** — Test components working together (e.g., engine + parser + reporter). The agent implementing the feature is expected to write these.
- **Acceptance tests** — High-level tests that validate end-to-end feature behavior. These are written separately and **must never be written by the agent implementing the feature**. However, all existing acceptance tests must pass for the feature to be considered done.

**Code coverage target: 80%.** Coverage is calculated from unit and integration tests only — acceptance tests must not be included in the coverage measurement. Write tests that verify meaningful behavior; do not write tests solely to inflate coverage numbers.

## Quality Checks

Security scans are required and must pass before a feature is considered done.

- **Dependency vulnerability scanning** — Use the OWASP Dependency-Check Maven plugin (`org.owasp:dependency-check-maven`) to scan for known vulnerabilities in third-party libraries. Run with `mvn dependency-check:check`. The build should fail if any CVE with a CVSS score >= 7 (high/critical) is found.
- **Static code analysis** — Use SpotBugs (`com.github.spotbugs:spotbugs-maven-plugin`) to detect common vulnerability patterns in our code (e.g., SQL injection, path traversal, deserialization issues). Run with `mvn spotbugs:check`.

Both plugins must be configured in the `pom.xml`. To run all quality checks together:

```bash
mvn verify                # Runs tests + dependency-check + spotbugs
```

## Plans

**The plan is always the first step.** Before any implementation begins on a new feature, you must write and commit a plan document. No code should be written, no files created or modified, until the plan exists and has been reviewed.

Create the plan document in the `plans/` directory. The filename should follow this format:

```
plans/YYYYMMDD-HHMMSS-<feature-name>.md
```

For example: `plans/20260226-143000-html-reporter.md`

The plan should cover:
- **Goal** — What the feature does and why
- **Affected files** — Which files will be created or modified
- **Approach** — Key design decisions and implementation steps
- **Testing** — How the feature will be tested
- **Open questions** — Anything that needs clarification before starting
