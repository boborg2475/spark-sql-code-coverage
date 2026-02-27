# High-Level Design: Spark SQL Code Coverage

## 1. Purpose

### Problem Statement

SQL-based data pipelines built on Apache Spark lack tooling for measuring test coverage at the expression level. Existing approaches verify only that queries execute without errors — they do not identify which specific branches, predicates, and conditional expressions are exercised by test data.

### What This Tool Does

Spark SQL Code Coverage measures **expression-level test coverage** for Spark SQL queries. Given `.sql` source files defining SQL logic and CSV files providing test data, the tool:

1. Parses source SQL to identify all **coverable expressions** — CASE branches, WHERE predicates, JOIN conditions, HAVING clauses, COALESCE/IF/NULLIF branches
2. Generates **check queries** for each coverable expression and evaluates them against the test data
3. Reports which expressions are **covered** (test data triggers the branch) and which are **uncovered**
4. Traces **column-level lineage** through SQL transformations, enriching coverage reports with data flow context

### Target Users

- Data engineers maintaining Spark SQL pipelines who want to measure and improve test data quality
- QA/testing teams validating that test datasets exercise all intended code paths
- CI/CD pipelines that need a pass/fail coverage gate for SQL changes

### Success Criteria

- Correctly identifies all coverable expression types listed above
- Accurately determines coverage by evaluating check queries against test data
- Produces CLI, HTML, and JSON reports with per-expression, per-statement, per-file, and overall coverage percentages
- Supports a `--fail-under` threshold for CI/CD integration
- Detects optimizer-pruned branches (Catalyst engine mode)

## 2. System Context

### External Dependencies

| Dependency | Version | Role | Scope |
|---|---|---|---|
| Apache Spark | 3.5.x | CatalystSqlParser for SQL parsing, SparkSession for query evaluation, QueryExecutionListener for plan analysis | provided |
| Scala | 2.13 | Implementation language | compile |
| Maven | 3.x | Build system (scala-maven-plugin, shade plugin) | build |
| Jackson (YAML) | (bundled with Spark) | YAML config file parsing | runtime (via Spark) |
| scopt | latest | CLI argument parsing | compile |
| ScalaTest | latest | Testing framework | test |
| OWASP dependency-check | latest | Dependency vulnerability scanning | build plugin |
| SpotBugs | latest | Static code analysis | build plugin |

### Input/Output Boundaries

```
                    ┌──────────────────────────────┐
  .sql files ──────►│                              │──────► CLI report (stdout)
                    │   Spark SQL Code Coverage    │──────► HTML report (file)
  .csv files ──────►│                              │──────► JSON report (file)
                    │                              │──────► Exit code (0/1)
  config.yaml ────►│                              │
  CLI args ────────►│                              │
                    └──────────────────────────────┘
```

**Inputs:**
- `.sql` source files — SQL statements to analyze for coverable expressions
- `.csv` data files — test data loaded into Spark tables for coverage evaluation
- `coverage-config.yaml` (optional) — YAML file mapping table names to CSV file paths
- CLI arguments — source dir, data dir, config path, output dir, format(s), engine choice, fail-under threshold, verbose flag

**Outputs:**
- Coverage reports in one or more formats (CLI/HTML/JSON)
- Process exit code: 0 if coverage meets threshold, 1 otherwise

## 3. Architecture Overview

### Two-Engine Design

The system uses a `CoverageEngine` trait with two implementations, sharing all parsing, extraction, and reporting infrastructure.

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLI Entry Point                          │
│              (SparkSqlCoverage + CoverageConfig)                │
└──────────┬──────────────────────────────────┬───────────────────┘
           │                                  │
           ▼                                  ▼
┌─────────────────────┐            ┌─────────────────────┐
│   CoverageEngine    │            │  CoverageReporter   │
│       (trait)       │            │      (trait)         │
├─────────────────────┤            ├─────────────────────┤
│ analyzeCoverage()   │            │ report()            │
│ extractCoverable    │            │ formatName          │
│   Expressions()     │            │                     │
└────┬───────────┬────┘            └──┬──────┬───────┬───┘
     │           │                    │      │       │
     ▼           ▼                    ▼      ▼       ▼
┌─────────┐ ┌──────────┐      ┌─────┐ ┌──────┐ ┌──────┐
│  Basic  │ │ Catalyst │      │ CLI │ │ HTML │ │ JSON │
│ Engine  │ │  Engine  │      └─────┘ └──────┘ └──────┘
└─────────┘ └──────────┘
     │           │
     │     extends + adds
     │     QueryExecution
     │     Listener
     │           │
     ▼           ▼
┌─────────────────────────────────────────────┐
│              Shared Components               │
├──────────────────┬──────────────────────────┤
│ SqlFileParser    │ Parse .sql → statements  │
│ ExpressionEx-    │ Walk AST → coverable     │
│   tractor        │   expressions            │
│ BranchCheck-     │ Generate check queries   │
│   Generator      │   per expression         │
│ DataSourceConfig │ Resolve table→CSV maps   │
│ LineageTracer    │ Column-level lineage     │
└──────────────────┴──────────────────────────┘
```

### Data Flow

```
.sql files ──► SqlFileParser ──► Seq[SqlStatement]
                                        │
                                        ▼
                              CatalystSqlParser.parsePlan()
                                        │
                                        ▼
                              ExpressionExtractor ──► Seq[CoverableExpression]
                              LineageTracer ────────► LineageGraph
                                        │
                                        ▼
                              BranchCheckGenerator ──► Batched check queries
                                        │
                                        ▼
.csv files ──► DataSourceConfig ──► SparkSession (temp tables)
                                        │
                                        ▼
                              Execute check queries ──► coverage counts
                                        │
                                        ▼
                              Aggregate ──► CoverageResult
                                        │
                                        ▼
                              CoverageReporter(s) ──► CLI / HTML / JSON output
```

### BasicCoverageEngine

1. Creates a local SparkSession
2. Loads CSVs into temp tables via DataSourceConfig
3. Parses source SQL → extracts coverable expressions + lineage + surrounding context
4. Generates batched check queries (one query per table/context combination)
5. Executes batched queries — each SUM column > 0 means that expression is covered
6. Aggregates results into CoverageResult

### CatalystCoverageEngine

Extends BasicCoverageEngine and adds:

7. Registers a QueryExecutionListener on the SparkSession
8. Executes the full source SQL against the test data
9. Captures analyzedPlan and optimizedPlan from QueryExecution
10. Compares plans: if the optimizer constant-folded or pruned a branch, marks it as "structurally present but unreachable with this data"
11. Enriches CoverageResult with pruned branch annotations

### Engine Comparison

| Aspect | Basic | Catalyst |
|---|---|---|
| Coverage method | Check queries only | Check queries + plan analysis |
| Pruned branch detection | No | Yes (analyzed vs optimized plan) |
| Complexity | Simple | More complex |
| Best for | Standard coverage needs | Deep analysis, unreachable branch detection |

## 4. Data Model Summary

### Type Relationships

```
CoverageResult
├── files: Seq[FileCoverage]
│   └── statements: Seq[StatementCoverage]
│       └── expressions: Seq[ExpressionCoverage]
│           └── expression: CoverableExpression
│               └── lineageColumns: Set[ColumnRef]
├── byExpressionType: Map[ExpressionType, TypeCoverage]
├── lineageGraph: LineageGraph
│   └── edges: Seq[LineageEdge]
│       ├── output: ColumnRef
│       └── inputs: Set[ColumnRef]
└── coveragePercent: Double
```

### Core Types

- **CoverageResult** — Top-level aggregate. Contains per-file results, breakdown by expression type, overall lineage graph, and overall coverage percentage.
- **FileCoverage** — Coverage results for a single `.sql` file. Contains per-statement results and file-level coverage percentage.
- **StatementCoverage** — Coverage results for a single SQL statement within a file. Contains the SqlStatement, per-expression results, and statement-level coverage percentage.
- **ExpressionCoverage** — Coverage result for a single coverable expression. Contains the CoverableExpression, its check query, and whether it was covered.
- **CoverableExpression** — A single coverable item extracted from the AST. Carries expression type, SQL fragment, source location, context, and lineage column dependencies.
- **SqlStatement** — A parsed SQL statement with its source file, line number, and statement index.
- **ExpressionType** — Sealed trait enum: `CASE_BRANCH`, `CASE_ELSE`, `WHERE_PREDICATE`, `JOIN_CONDITION`, `HAVING_PREDICATE`, `COALESCE_BRANCH`, `IF_BRANCH`, `NULLIF_BRANCH`, `SUBQUERY_PREDICATE`, `FILTER_PREDICATE`.

### Lineage Types

- **ColumnRef** — Reference to a column, optionally qualified by table name.
- **LineageEdge** — Maps one output column to its input column references, with optional transformation SQL.
- **LineageGraph** — Collection of lineage edges plus a mapping from expression IDs to their dependent columns.

## 5. Key Design Decisions

### 5.1 Batched Check Queries

**Decision:** Batch all check conditions for the same table/context combination into a single SELECT with multiple SUM(CASE WHEN ...) columns.

**Rationale:** Without batching, each coverable expression requires a separate `SELECT COUNT(*)` query, meaning N table scans for N expressions. Batching reduces this to one scan per table/context combination. For SQL files with many CASE branches in the same context, this is a significant performance win.

**Example:**
```sql
SELECT
  SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END),
  SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
  SUM(CASE WHEN NOT(status='active') AND NOT(status='pending') THEN 1 ELSE 0 END)
FROM orders
WHERE region = 'US' AND amount > 50
```

### 5.2 Expression.sql for AST-to-String Conversion

**Decision:** Use Catalyst's built-in `Expression.sql` method to convert AST nodes back to SQL strings for check queries, rather than building a custom AST-to-string serializer.

**Rationale:** Catalyst already provides exact Spark SQL syntax via `Expression.sql`. Reusing it avoids the maintenance burden and correctness risk of a custom serializer, and guarantees the generated check queries use syntax that Spark can parse back.

### 5.3 Lineage as a First-Class Concept

**Decision:** Design lineage into the core data model from day one. Each `CoverableExpression` carries `lineageColumns: Set[ColumnRef]` at creation time, and the top-level `CoverageResult` includes a `LineageGraph`.

**Rationale:** Bolting lineage on after the fact would require threading column dependency information through layers that weren't designed for it. By including it in the initial model, lineage enriches coverage reports naturally (showing which source columns affect each branch) and supports future cross-statement tracking.

### 5.4 Two-Engine Strategy

**Decision:** Provide two engine implementations behind a shared trait — a simpler BasicCoverageEngine and a more advanced CatalystCoverageEngine.

**Rationale:** The basic engine covers the majority use case (does test data hit this branch?) with straightforward check queries. The Catalyst engine adds optimizer-aware pruned branch detection, which is more complex but more precise. Users choose the level of analysis they need. The shared trait keeps reporters and the CLI agnostic to engine choice.

### 5.5 Convention-Based Config Fallback

**Decision:** Table-to-CSV mapping supports explicit YAML config with a convention-based fallback: if table `orders` has no config entry, look for `<data-dir>/orders.csv`.

**Rationale:** Reduces boilerplate for simple projects where CSV filenames match table names. The explicit config handles cases where they don't match, or where the file format or path differs from convention.

### 5.6 Contextual Check Queries

**Decision:** Check queries for CASE branches include the surrounding WHERE/JOIN filters as context, so coverage is measured only against rows that actually reach the expression.

**Rationale:** A CASE branch inside `WHERE region = 'US'` should only be checked against rows where `region = 'US'`. Without context, a branch could appear covered by rows that never flow through the containing filter, producing false-positive coverage.

## 6. Component Inventory

| Component | Purpose | Key Inputs | Key Outputs |
|---|---|---|---|
| **SparkSqlCoverage** | CLI entry point; wires config, engine, reporters | CLI args | Process exit code |
| **CoverageConfig** | Parses CLI arguments via scopt | `Array[String]` | Typed config object |
| **DataSourceConfig** | Resolves table→CSV mappings from YAML + convention fallback | Config file path, data dir, required table names | `Map[String, Path]` |
| **SqlFileParser** | Reads `.sql` files, splits on semicolons, tracks line numbers | `.sql` file paths | `Seq[SqlStatement]` |
| **ExpressionExtractor** | Walks Catalyst AST to find coverable expressions | Parsed LogicalPlan | `Seq[CoverableExpression]` |
| **BranchCheckGenerator** | Generates batched check queries per expression | CoverableExpressions, source table | Batched check query SQL |
| **LineageTracer** | Extracts column-level lineage from LogicalPlan | Parsed LogicalPlan | `LineageGraph` |
| **BasicCoverageEngine** | Loads CSVs, runs check queries, aggregates results | Source files, data source map | `CoverageResult` |
| **CatalystCoverageEngine** | Extends Basic; detects optimizer-pruned branches | Source files, data source map | `CoverageResult` (with pruned annotations) |
| **CoverageReporter** | Trait for output formatting | `CoverageResult`, output path | Report output |
| **CliReporter** | ANSI-colored terminal output | `CoverageResult` | Formatted stdout |
| **HtmlReporter** | Highlighted SQL with covered/uncovered spans | `CoverageResult`, output path | HTML file |
| **JsonReporter** | Machine-readable JSON with lineage data | `CoverageResult`, output path | JSON file |

## 7. Non-Functional Requirements

### Performance

- **Batched queries** — Check conditions for the same table/context are batched into a single SELECT to minimize table scans
- **Local SparkSession** — Spark runs in local mode; no cluster overhead for typical usage
- **Proportional scaling** — Runtime scales with (number of SQL files × number of coverable expressions × test data size), not with arbitrary external factors

### Compatibility

- **Spark 3.5.x** — Targets the CatalystSqlParser and LogicalPlan APIs available in Spark 3.5
- **Scala 2.13** — Compiled against Scala 2.13 to match typical Spark 3.5 deployments
- **JVM 11+** — Runs on JVM 11 or later (Spark 3.5 requirement)

### Security

- **OWASP dependency-check** — Scans third-party dependencies for known CVEs; build fails if any CVE with CVSS >= 7 is found
- **SpotBugs static analysis** — Detects common vulnerability patterns (SQL injection, path traversal, deserialization issues) in project code
- Both checks run as part of `mvn verify`

### Test Coverage

- **80% code coverage target** from unit and integration tests (acceptance tests excluded from measurement)
- **Unit tests** — Each component tested in isolation
- **Integration tests** — End-to-end specs with realistic `.sql` and `.csv` fixtures for both engines
- **Acceptance tests** — High-level behavioral validation; written separately from feature implementation

### Distribution

Three packaging modes:
1. **Library JAR** — Core API for programmatic use; Spark is `provided` scope
2. **Thin CLI JAR** (default `mvn package`) — For use with `spark-submit`; ~10MB
3. **Fat CLI JAR** (`mvn package -Pstandalone`) — Self-contained with Spark shaded in; ~200MB+

## 8. Open Questions

1. **Cross-statement lineage** — The current design traces lineage within individual SQL statements. Tracing lineage across statement boundaries (e.g., through views or CTEs defined in earlier statements) is deferred to a future iteration. The data model supports it via `LineageGraph`, but the `LineageTracer` implementation will initially scope to single statements.

2. **DDL handling** — Source `.sql` files may contain DDL statements (CREATE TABLE, DROP TABLE, etc.) alongside DML. The tool must gracefully skip DDL statements rather than fail. The exact classification logic (which statement types are coverable vs. skippable) needs to be defined during implementation.

3. **Error recovery** — When a source SQL file contains a syntax error in one statement, should the tool skip that statement and continue with the rest of the file, or fail the entire file? The implementation plan implies graceful handling but does not specify the exact behavior. The recommended approach is: log a warning, skip the invalid statement, and continue processing.

4. **HAVING predicate check queries** — HAVING predicates require running the aggregate query to check coverage, which is more complex than simple WHERE-based checks. The exact batching strategy for HAVING checks (whether they can be combined with other checks) needs to be worked out during BranchCheckGenerator implementation.

5. **Subquery predicate coverage** — EXISTS, IN (subquery), and scalar subqueries involve nested query evaluation. The check query generation strategy for these expression types may need special handling beyond the standard SUM(CASE WHEN ...) pattern.
