# Spark SQL Code Coverage Tool - Implementation Plan

## Context

This project builds a tool that measures **expression-level test coverage** for Spark SQL. Given source `.sql` files defining SQL logic and CSV files providing test data, the tool:

1. Parses source SQL to identify all **coverable expressions** (CASE branches, WHERE predicates, JOIN conditions, HAVING clauses, COALESCE/IF/NULLIF branches)
2. Loads test data (CSVs) into Spark tables
3. Runs **check queries** (`SELECT COUNT(*) FROM table WHERE <branch_condition>`) to determine which branches the test data satisfies
4. Reports coverage per-query, aggregated per-file

Coverage means: "does the test data trigger this branch?" For example, if source SQL has `CASE WHEN status = 'active' THEN ...` and the test CSV contains a row with `status=active`, that branch is covered.

The tool uses **SQL lineage** (column-level data flow tracking) to trace which source columns flow through which transformations, enriching coverage reports with data flow context.

## Tech Stack

- **Scala 2.13** with **Maven** (scala-maven-plugin)
- **Spark 3.5.x** (CatalystSqlParser for SQL parsing, SparkSession for predicate evaluation, QueryExecutionListener for catalyst engine)
- **SnakeYAML** for config file parsing
- **scopt** for CLI argument parsing
- **ScalaTest** for testing
- Package namespace: `com.bob.sparkcoverage`

## Architecture Overview

```
CoverageEngine (trait) ─── two implementations:
├── BasicCoverageEngine     Parses SQL with CatalystSqlParser, loads CSVs into
│                           local SparkSession, evaluates branch predicates via
│                           check queries. Straightforward coverage determination.
└── CatalystCoverageEngine  Same as Basic, plus hooks into SparkSession via
                            QueryExecutionListener to capture resolved plans.
                            Compares analyzedPlan vs optimizedPlan to detect
                            pruned/unreachable branches. More precise.

Shared components:
├── ExpressionExtractor     Walks Catalyst AST to identify all coverable expressions
├── BranchCheckGenerator    Generates SELECT COUNT(*) check queries for each branch
├── SqlFileParser           Reads/splits .sql files into statements
├── DataSourceConfig        Maps CSV files to table names (YAML config + convention fallback)
├── LineageTracer           Column-level lineage through SQL transformations
└── CoverageReporter        CLI / HTML / JSON output
```

## Test Data Configuration

Table-to-CSV mapping uses a YAML config file with convention-based fallback:

**Config file (`coverage-config.yaml`)**:
```yaml
dataSources:
  orders: data/test_orders.csv
  customers: data/test_customers.csv
```

**Convention fallback**: If no config entry exists for table `orders`, look for `<data-dir>/orders.csv`.

## Project Structure

```
src/main/scala/com/bob/sparkcoverage/
├── SparkSqlCoverage.scala          # CLI entry point
├── config/
│   ├── CoverageConfig.scala        # CLI config + scopt parser
│   └── DataSourceConfig.scala      # Table-to-CSV mapping (YAML config + convention fallback)
├── model/
│   ├── CoverageModels.scala        # All coverage + lineage data classes (see below)
│   └── ExpressionType.scala        # Sealed trait: CASE_BRANCH, WHERE_PREDICATE, etc.
├── parser/
│   └── SqlFileParser.scala         # Read .sql files, split on semicolons, track line numbers
├── engine/
│   ├── CoverageEngine.scala        # Trait: analyzeCoverage(sourceFiles, dataSources)
│   ├── ExpressionExtractor.scala   # Walk LogicalPlan/Expression trees to find coverable exprs
│   ├── BranchCheckGenerator.scala  # Generate check queries for each coverable expression
│   ├── BasicCoverageEngine.scala   # SparkSession-based: load CSVs, run check queries
│   └── CatalystCoverageEngine.scala # Extends Basic + QueryExecutionListener for pruned branches
├── lineage/
│   └── LineageTracer.scala         # Build lineage graph from LogicalPlan
└── reporter/
    ├── CoverageReporter.scala      # Trait: report(), formatName
    ├── CliReporter.scala           # ANSI-colored terminal output
    ├── HtmlReporter.scala          # Highlighted SQL with covered/uncovered spans
    └── JsonReporter.scala          # Machine-readable JSON for CI/CD

src/main/resources/html/
├── report-template.html
└── style.css

src/test/scala/com/bob/sparkcoverage/
├── parser/SqlFileParserSpec.scala
├── engine/
│   ├── ExpressionExtractorSpec.scala
│   ├── BranchCheckGeneratorSpec.scala
│   ├── BasicCoverageEngineSpec.scala
│   └── CatalystCoverageEngineSpec.scala
├── lineage/LineageTracerSpec.scala
├── reporter/{Cli,Html,Json}ReporterSpec.scala
└── integration/
    ├── EndToEndBasicSpec.scala
    └── EndToEndCatalystSpec.scala

src/test/resources/
├── sql/source/   (simple_case.sql, complex_joins.sql, nested_subqueries.sql)
└── data/         (products.csv, orders.csv, customers.csv)
```

## Key Interfaces

### CoverageEngine (trait)
```scala
trait CoverageEngine {
  def analyzeCoverage(
    sourceFiles: Seq[Path],
    dataSources: Map[String, Path]  // tableName -> CSV path
  ): CoverageResult

  def extractCoverableExpressions(sql: String): Seq[CoverableExpression]
}
```

### CoverageReporter (trait)
```scala
trait CoverageReporter {
  def report(result: CoverageResult, outputPath: Option[Path]): Unit
  def formatName: String
}
```

### DataSourceConfig
```scala
case class DataSource(tableName: String, filePath: Path, format: String = "csv")

object DataSourceConfig {
  // Load from YAML config, fall back to convention (tableName.csv in dataDir)
  def resolve(configFile: Option[Path], dataDir: Path, requiredTables: Set[String]): Map[String, Path]
}
```

## Core Data Model

### ExpressionType (sealed trait)
`CASE_BRANCH`, `CASE_ELSE`, `WHERE_PREDICATE`, `JOIN_CONDITION`, `HAVING_PREDICATE`, `COALESCE_BRANCH`, `IF_BRANCH`, `NULLIF_BRANCH`, `SUBQUERY_PREDICATE`, `FILTER_PREDICATE`

### Lineage Model (designed alongside coverage model)
```
ColumnRef(table: Option[String], column: String)

LineageEdge(output: ColumnRef, inputs: Set[ColumnRef], transformationSql: Option[String])

LineageGraph(edges: Seq[LineageEdge], expressionColumns: Map[String, Set[ColumnRef]])
```

### Coverage Model Hierarchy
```
CoverageResult (top-level aggregate)
├── files: Seq[FileCoverage]
│   ├── filePath: String
│   ├── statements: Seq[StatementCoverage]
│   │   ├── statement: SqlStatement (sql, sourceFile, lineNumber, statementIdx)
│   │   ├── expressions: Seq[ExpressionCoverage]
│   │   │   ├── expression: CoverableExpression
│   │   │   │     (id, expressionType, context, sqlFragment, sourceFile, lineNumber,
│   │   │   │      lineageColumns: Set[ColumnRef])   ← lineage baked in from the start
│   │   │   ├── isCovered: Boolean
│   │   │   └── checkQuery: String   ← the SQL used to verify coverage
│   │   └── coveragePercent: Double
│   └── coveragePercent: Double
├── byExpressionType: Map[ExpressionType, TypeCoverage]
├── lineageGraph: LineageGraph   ← top-level lineage for the entire file
└── coveragePercent: Double (overall)
```

## How Coverage Determination Works

### Core Algorithm (both engines)

1. **Parse** source .sql files into SQL statements
2. **Extract** coverable expressions by walking the Catalyst AST
3. **Build lineage** for each statement (which columns flow through which expressions)
4. For each coverable expression, **generate a contextual check query** that includes the surrounding WHERE/JOIN filters so we only test branches against rows that actually reach the expression:
   - `CASE WHEN status = 'active'` inside `WHERE region = 'US' AND amount > 50`
     → `SELECT COUNT(*) FROM orders WHERE region = 'US' AND amount > 50 AND status = 'active'`
   - `CASE ELSE` (same context)
     → `SELECT COUNT(*) FROM orders WHERE region = 'US' AND amount > 50 AND NOT(status = 'active') AND NOT(status = 'pending')`
   - `WHERE price > 100` → `SELECT COUNT(*) FROM <table> WHERE price > 100`
   - `JOIN ... ON a.id = b.id` → `SELECT COUNT(*) FROM a JOIN b ON a.id = b.id`
5. **Batch** all check conditions for the same table/context into a single query:
   ```sql
   SELECT
     SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END),
     SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END),
     SUM(CASE WHEN NOT(status='active') AND NOT(status='pending') THEN 1 ELSE 0 END)
   FROM orders
   WHERE region = 'US' AND amount > 50
   ```
   One table scan covers all branches instead of N separate queries.
6. **Load CSVs** into Spark temp tables
7. **Execute** batched check queries: for each column, if `SUM > 0`, that expression is **covered**
8. **Aggregate** into CoverageResult

### Basic Engine

```
1. Create local SparkSession
2. Load CSVs into temp tables
3. Parse source SQL → extract coverable expressions + lineage + surrounding context
4. Generate batched check queries via BranchCheckGenerator (one query per table/context)
5. Execute batched queries:
   spark.sql("SELECT SUM(CASE WHEN cond1 ...), SUM(CASE WHEN cond2 ...) FROM table WHERE <context>")
6. For each SUM column: if > 0, that branch is covered
7. Aggregate results into CoverageResult
```

### Catalyst Engine (extends Basic)

```
1-6. Same as Basic Engine
7. Additionally: register QueryExecutionListener
8. Execute the full source SQL against the test data
9. Capture analyzedPlan and optimizedPlan from QueryExecution
10. Compare plans: if optimizer constant-folded or pruned a branch,
    mark it as "structurally present but unreachable with this data"
11. Enrich CoverageResult with pruned branch annotations
```

### Key Difference

| Aspect | Basic | Catalyst |
|--------|-------|----------|
| SQL engine | SparkSession (local) | SparkSession (local) |
| Coverage method | Check queries only | Check queries + plan analysis |
| Pruned branch detection | No | Yes (analyzed vs optimized plan) |
| Complexity | Simple | More complex |
| Best for | Standard coverage needs | Deep analysis, unreachable branch detection |

## How Lineage Is Used

Lineage is a **first-class concept** designed alongside the core data model (not bolted on later).

1. **Build lineage graph** during expression extraction: each `CoverableExpression` carries its `lineageColumns: Set[ColumnRef]` — the source columns it depends on
2. **Enrich coverage reports**: Show which source columns are involved in each covered/uncovered expression
3. **Identify dead code**: Source columns that appear in no coverable expression
4. **Cross-statement tracking** (future): Trace lineage through view boundaries

### Lineage Extraction

The `LineageTracer` walks the LogicalPlan alongside the `ExpressionExtractor`:
- `Project(projectList, child)` → each projection maps output column to input column references
- `Filter(condition, child)` → condition references input columns
- `Join(left, right, _, condition, _)` → condition references columns from both sides
- `Aggregate(groupExprs, aggExprs, child)` → aggregate expressions map to input columns

Each `CoverableExpression` is tagged with its column dependencies at creation time.

## Expression Extraction Details

The `ExpressionExtractor` walks the Catalyst AST in two passes:

**Plan-level** (identifies context):
- `Filter(condition, child)` → WHERE or HAVING (HAVING if child is `Aggregate`)
- `Join(left, right, _, condition, _)` → JOIN condition
- `Project(projectList, child)` → inspect projections for CASE/IF/COALESCE
- `Aggregate(_, aggExprs, _)` → inspect aggregate expressions

**Expression-level** (identifies coverable items):
- `CaseWhen(branches, elseValue)` → each branch = `CASE_BRANCH`, else = `CASE_ELSE`
- `If(pred, trueVal, falseVal)` → `IF_BRANCH` entries
- `Coalesce(children)` → each child = `COALESCE_BRANCH`
- `NullIf(left, right)` → `NULLIF_BRANCH`
- `Exists/ListQuery/ScalarSubquery` → `SUBQUERY_PREDICATE`
- Compound predicates (And/Or) → decompose into individual sub-predicates

## BranchCheckGenerator Details

Instead of complex AST-to-SQL reconstruction, the `BranchCheckGenerator` constructs simple check queries:

```scala
trait BranchCheckGenerator {
  def generateCheckQueries(
    expression: CoverableExpression,
    sourceTable: String
  ): Seq[CheckQuery]
}

case class CheckQuery(
  expressionId: String,
  sql: String,           // e.g., "SELECT COUNT(*) FROM orders WHERE status = 'active'"
  description: String    // Human-readable description of what's being checked
)
```

**Generation rules** (all include surrounding context filters):
- **CASE_BRANCH**: `SUM(CASE WHEN <when_condition> THEN 1 ELSE 0 END)` within `FROM <table> WHERE <context>`
- **CASE_ELSE**: `SUM(CASE WHEN NOT(<when1>) AND NOT(<when2>) THEN 1 ELSE 0 END)` within same context
- **WHERE_PREDICATE**: `SUM(CASE WHEN <predicate> THEN 1 ELSE 0 END)` from the raw table
- **JOIN_CONDITION**: `SELECT COUNT(*) FROM <left> JOIN <right> ON <condition>` (standalone, not batchable)
- **HAVING_PREDICATE**: Run the aggregate query with HAVING, check if result is non-empty
- **COALESCE_BRANCH**: For each arg position, check if prior args are NULL and this arg is non-NULL
- **IF_BRANCH**: `SUM(CASE WHEN <condition> THEN 1 ELSE 0 END)` (true branch), `SUM(CASE WHEN NOT(<condition>) THEN 1 ELSE 0 END)` (false branch)

All branch checks for the same table+context are **batched into a single SELECT** with multiple SUM columns, so one table scan covers all branches.

The key insight: we use `Expression.sql` from Catalyst to get the SQL string for each condition, which gives us exact Spark SQL syntax. This avoids manual AST-to-string reconstruction.

## Implementation Phases

### Phase 1: Foundation
- `pom.xml` (Scala 2.13, Spark 3.5.x, SnakeYAML, scopt, ScalaTest, plugins)
- `ExpressionType.scala` + `CoverageModels.scala` (all data model case classes **including lineage models**)
- `SqlFileParser.scala` (read .sql files, split on semicolons, track line numbers)
- `DataSourceConfig.scala` (YAML config parsing + convention-based fallback)
- `SqlFileParserSpec.scala`
- **Deliverable**: Can read .sql files → `Seq[SqlStatement]` and resolve table → CSV mappings

### Phase 2: Expression Extraction + Lineage
- `ExpressionExtractor.scala` (walk LogicalPlan via CatalystSqlParser)
- `LineageTracer.scala` (extract column-level lineage alongside expressions)
- Each `CoverableExpression` carries its `lineageColumns` from creation
- `ExpressionExtractorSpec.scala` + `LineageTracerSpec.scala`
- Test fixtures: .sql files in test resources
- **Deliverable**: Given SQL, produce `Seq[CoverableExpression]` with lineage metadata

### Phase 3: Check Query Generation + Basic Engine
- `BranchCheckGenerator.scala` (generate `SELECT COUNT(*)` check queries per expression)
- `BasicCoverageEngine.scala` (load CSVs into Spark, run check queries, aggregate)
- `BranchCheckGeneratorSpec.scala` + `BasicCoverageEngineSpec.scala`
- Test fixtures: .csv data files in test resources
- **Deliverable**: `basicEngine.analyzeCoverage(sqlFiles, dataSources)` → `CoverageResult`

### Phase 4: Reporters
- `CliReporter.scala` (ANSI-colored terminal output, color-coded by threshold)
- `JsonReporter.scala` (structured JSON for CI/CD, includes lineage data)
- `HtmlReporter.scala` (highlighted SQL with covered/uncovered CSS spans, lineage annotations)
- HTML template + CSS resources
- Reporter specs
- **Deliverable**: All three report formats working, with lineage context in output

### Phase 5: CLI Entry Point
- `CoverageConfig.scala` (scopt OParser: --source-dir, --data-dir, --config, --output-dir, --format, --engine, --fail-under, --verbose)
- `SparkSqlCoverage.scala` (main method wiring everything)
- Shade plugin config for fat JAR
- **Deliverable**: `java -jar spark-sql-coverage.jar --source-dir ./sql --data-dir ./data --format cli,html,json`

### Phase 6: Catalyst Runtime Engine
- `CatalystCoverageEngine.scala` (extends BasicCoverageEngine + QueryExecutionListener for pruned branch detection)
- `CatalystCoverageEngineSpec.scala` (integration test with local Spark)
- **Deliverable**: Catalyst engine adds pruned/unreachable branch annotations to coverage results

### Phase 7: Integration & Polish
- `EndToEndBasicSpec.scala` + `EndToEndCatalystSpec.scala`
- Edge cases: empty files, syntax errors, DDL-only files, nested subqueries
- Library packaging (publish core API without CLI wrapper)
- README documentation
- **Deliverable**: Both engines work end-to-end, library + CLI distribution

## Distribution

The tool is packaged three ways:
1. **Library**: Core API jar (`com.bob.sparkcoverage`) that users add as a test dependency. They call `CoverageEngine.analyzeCoverage()` from their test suites. Spark is `provided` scope.
2. **Thin CLI JAR** (for spark-submit): Small jar (~10MB) without Spark bundled. Users run via `spark-submit --class com.bob.sparkcoverage.SparkSqlCoverage spark-sql-coverage.jar ...`. Requires Spark installed.
3. **Fat CLI JAR** (standalone): Shaded jar (~200MB+) with Spark bundled. Fully self-contained: `java -jar spark-sql-coverage-standalone.jar ...`. No Spark install needed.

Maven profiles control which JARs are built:
- `mvn package` → thin jar (default)
- `mvn package -Pstandalone` → fat jar with Spark shaded in

## Verification

1. **Unit tests**: `mvn test` — ScalaTest specs for each component
2. **Integration tests**: EndToEnd specs with realistic .sql files + .csv test data
3. **Manual test**: Build fat JAR, run against sample data:
   ```
   java -jar target/spark-sql-coverage.jar \
     --source-dir ./sample/sql \
     --data-dir ./sample/data \
     --format cli,html,json \
     --output-dir ./target/coverage
   ```
4. **Verify both engines produce consistent results** for the same source SQL + test data
5. **Edge cases**: Empty files, syntax errors (graceful handling), DDL-only files, deeply nested subqueries, CSVs with missing columns
