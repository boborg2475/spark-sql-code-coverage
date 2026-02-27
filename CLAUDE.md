# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spark SQL Code Coverage — a Scala tool that measures **expression-level test coverage** for Spark SQL queries. Given `.sql` source files and CSV test data, it identifies coverable expressions (CASE branches, WHERE predicates, JOIN conditions, etc.), evaluates whether test data satisfies each expression, and generates coverage reports.

Package namespace: `com.bob.sparkcoverage`

## Tech Stack

- **Scala 2.13** with **Maven** (scala-maven-plugin)
- **Apache Spark 3.5.x** — CatalystSqlParser for SQL parsing, SparkSession for predicate evaluation, QueryExecutionListener for catalyst engine (provided scope)
- **Jackson** — YAML config file parsing (bundled with Spark, no extra dependency)
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

Two-engine design behind a shared `CoverageEngine` trait. See `plans/implementation-plan.md` for detailed project structure, data models, key interfaces, and algorithm specifics.

- **BasicCoverageEngine** — SparkSession-based. Loads CSVs, generates check queries, evaluates coverage.
- **CatalystCoverageEngine** — Extends Basic. Uses QueryExecutionListener to detect optimizer-pruned branches.

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

## Branching

**Never push directly to `main`.** All work must happen on feature branches and be merged via pull requests.

- Create a feature branch for each feature: `feature/<feature-name>` (e.g., `feature/html-reporter`)
- When the feature is complete, create a PR against `main` using `gh pr create`
- Do not merge your own PR — leave it for review
- **Commit messages**: Do not add `Co-Authored-By` lines for Claude. They are unnecessary.

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
