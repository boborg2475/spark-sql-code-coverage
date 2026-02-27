# Low-Level Design: Phase 1 — Foundation & Data Pipeline

## 1. Goal

Prove the core infrastructure works end-to-end: parse SQL files, load CSV test data into Spark tables, execute SQL against them, and capture results with robust error handling. Phase 1 delivers a working data pipeline that later phases build coverage analysis on top of.

## 2. Component Inventory

| Component | File | Purpose |
|---|---|---|
| `ExpressionType` | `model/ExpressionType.scala` | Sealed trait with 10 coverable expression types |
| `CoverageModels` | `model/CoverageModels.scala` | All data model case classes (SqlStatement, coverage hierarchy, lineage types, DataSource, ExecutionResult, ExecutionStatus) |
| `SqlFileParser` | `parser/SqlFileParser.scala` | Reads `.sql` files, strips comments, splits on semicolons, tracks line numbers |
| `DataSourceConfig` | `config/DataSourceConfig.scala` | YAML config parsing + convention-based `tableName.csv` fallback |
| `DataLoader` | `engine/DataLoader.scala` | Loads CSV files into Spark temporary views |
| `SqlExecutor` | `engine/SqlExecutor.scala` | Executes SQL statements against Spark, captures results, handles errors |

### File Layout After Phase 1

```
src/main/scala/com/bob/sparkcoverage/
├── config/
│   └── DataSourceConfig.scala
├── engine/
│   ├── DataLoader.scala
│   └── SqlExecutor.scala
├── model/
│   ├── CoverageModels.scala
│   └── ExpressionType.scala
└── parser/
    └── SqlFileParser.scala

src/test/scala/com/bob/sparkcoverage/
├── acceptance/
│   ├── CoverageModelsAcceptanceSpec.scala
│   ├── DataSourceConfigAcceptanceSpec.scala
│   ├── Phase1IntegrationAcceptanceSpec.scala
│   └── SqlFileParserAcceptanceSpec.scala
├── config/
│   └── DataSourceConfigSpec.scala
├── engine/
│   ├── DataLoaderSpec.scala
│   └── SqlExecutorSpec.scala
├── integration/
│   └── DataPipelineIntegrationSpec.scala
└── parser/
    └── SqlFileParserSpec.scala

src/test/resources/
├── acceptance/
│   ├── config/
│   ├── data/
│   └── sql/
├── data/
│   ├── orders.csv
│   ├── customers.csv
│   ├── products.csv
│   ├── empty.csv
│   └── malformed.csv
└── sql/
    ├── simple_select.sql
    ├── multi_statement.sql
    ├── ddl_mixed.sql
    ├── syntax_error.sql
    └── missing_table.sql
```

## 3. Detailed Design

### 3.1 Execution Models (`CoverageModels.scala`)

```scala
/** Status of a single SQL statement execution. */
sealed trait ExecutionStatus
object ExecutionStatus {
  case object Success extends ExecutionStatus
  case object Skipped extends ExecutionStatus   // DDL or non-coverable statement
  case object Error extends ExecutionStatus     // Syntax error, missing table, etc.
}

/** Result of executing a single SQL statement against Spark. */
case class ExecutionResult(
  statement: SqlStatement,
  status: ExecutionStatus,
  rowCount: Option[Long],       // Number of rows returned (Success only)
  schema: Option[Seq[String]],  // Column names from result schema (Success only)
  error: Option[String]         // Error message (Error status only)
)
```

**Design decisions:**

- `schema` stores column names as `Seq[String]` rather than Spark's `StructType` to avoid leaking Spark types into the model layer. This keeps the models serializable and testable without a Spark dependency.
- `rowCount` and `schema` are `Option` because they are only populated on successful SELECT-type executions.
- `error` captures the exception message for diagnostics. Full stack traces go to the logger, not the model.

### 3.2 DataLoader

**File:** `src/main/scala/com/bob/sparkcoverage/engine/DataLoader.scala`

**Responsibility:** Given a `Map[String, Path]` (table name to CSV path) and a `SparkSession`, load each CSV file into a temporary view with the corresponding table name.

```scala
package com.bob.sparkcoverage.engine

import java.nio.file.Path
import org.apache.spark.sql.SparkSession

object DataLoader {

  case class LoadResult(
    tableName: String,
    path: Path,
    success: Boolean,
    rowCount: Option[Long],
    error: Option[String]
  )

  def loadAll(
    dataSources: Map[String, Path],
    spark: SparkSession
  ): Seq[LoadResult]
}
```

**Algorithm:**

1. For each `(tableName, csvPath)` entry in `dataSources`:
   a. Verify the file exists. If not, produce a `LoadResult` with `success = false` and an error message. Do not throw — continue to the next table.
   b. Call `spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath.toString)` to read the CSV.
   c. Register the DataFrame as a temporary view: `df.createOrReplaceTempView(tableName)`.
   d. Count rows via `df.count()` and return a `LoadResult` with `success = true`.
   e. If any exception occurs during read/register, catch it and produce a `LoadResult` with `success = false`.
2. Return all `LoadResult`s.

**CSV loading options:**

| Option | Value | Rationale |
|---|---|---|
| `header` | `true` | All test CSV files have a header row with column names |
| `inferSchema` | `true` | Automatically detect column types (numeric, string, boolean) from the data. Avoids requiring an explicit schema definition for each table. |

**Error scenarios:**

| Scenario | Behavior |
|---|---|
| CSV file does not exist | `LoadResult(success=false, error="File not found: ...")` |
| CSV file is empty (no data rows) | Success — registers view with 0 rows. `rowCount = Some(0)`. |
| CSV file is malformed (inconsistent columns) | Spark's CSV reader tolerates this by default (`PERMISSIVE` mode). Loads with nulls for missing fields. Returns success. |
| Duplicate table name | `createOrReplaceTempView` replaces silently. Last mapping wins. |

### 3.3 SqlExecutor

**File:** `src/main/scala/com/bob/sparkcoverage/engine/SqlExecutor.scala`

**Responsibility:** Given a sequence of `SqlStatement`s and a `SparkSession` (with tables already loaded), execute each statement and return structured results. Handles DDL classification, per-statement error recovery, and result capture.

```scala
package com.bob.sparkcoverage.engine

import com.bob.sparkcoverage.model.CoverageModels.{ExecutionResult, ExecutionStatus, SqlStatement}
import org.apache.spark.sql.SparkSession

object SqlExecutor {

  def executeAll(
    statements: Seq[SqlStatement],
    spark: SparkSession
  ): Seq[ExecutionResult]

  /** Classify a SQL statement as DDL (skippable) or DML (executable). */
  private[engine] def isDdl(sql: String): Boolean
}
```

**Algorithm:**

1. For each `SqlStatement` in the input sequence:
   a. **Normalize:** Trim the SQL and extract the first keyword (uppercase).
   b. **Classify:** Call `isDdl(sql)`. If DDL, return `ExecutionResult` with `status = Skipped`.
   c. **Execute:** Call `spark.sql(statement.sql)` to get a `DataFrame`.
   d. **Capture:** Call `df.count()` for row count. Extract column names from `df.schema.fieldNames`.
   e. **Error recovery:** If `spark.sql(...)` or `df.count()` throws an exception, catch it and return `ExecutionResult` with `status = Error` and the exception message. Continue processing the next statement.
2. Return all `ExecutionResult`s.

**DDL Classification (`isDdl`):**

Uses first-keyword heuristic on the trimmed, uppercased SQL:

| First keyword(s) | Classification |
|---|---|
| `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `WITH` | DML — execute it |
| `CREATE`, `DROP`, `ALTER`, `TRUNCATE`, `GRANT`, `REVOKE`, `SET`, `USE`, `DESCRIBE`, `SHOW`, `EXPLAIN`, `MSCK`, `REFRESH`, `CACHE`, `UNCACHE`, `ADD`, `LIST` | DDL/utility — skip it |
| Anything else | Attempt execution; handle errors |

The first-keyword approach is simple and sufficient for Phase 1. Phase 2's expression extractor will use Catalyst's parsed plan types for definitive classification, but the data pipeline layer does not need that precision — it just needs to avoid crashing on DDL.

**Error scenarios:**

| Scenario | Behavior |
|---|---|
| Syntax error in SQL | `spark.sql()` throws `ParseException`. Caught → `ExecutionResult(status=Error, error="...")`. Next statement continues. |
| Missing table reference | `spark.sql()` throws `AnalysisException` ("Table or view not found"). Caught → `ExecutionResult(status=Error, error="...")`. Next statement continues. |
| Missing column reference | Same — `AnalysisException`, caught and recorded. |
| DDL statement | Skipped without execution. `ExecutionResult(status=Skipped)`. |
| Empty SQL string | Should not reach executor (filtered by SqlFileParser), but if it does: skip it. |
| Statement that returns no rows | Success with `rowCount = Some(0)`. |
| Ambiguous column reference | Spark resolves or throws `AnalysisException`. Caught if thrown. |

**Why per-statement recovery matters:** A SQL source file may contain a mix of valid and invalid statements (e.g., a DDL preamble, a query referencing a table not in test data, and several valid queries). The executor must not abort the entire file on the first error — it reports each failure and continues, so downstream coverage analysis can still process the valid statements.

### 3.4 SparkSession Management

Phase 1 does not create a SparkSession itself — tests will create `SparkSession.builder().master("local[*]").appName("test").getOrCreate()` in their test setup. The `DataLoader` and `SqlExecutor` receive a `SparkSession` parameter.

In Phase 2, the `BasicCoverageEngine` will own SparkSession creation as part of its lifecycle. Phase 1 components are designed to be engine-agnostic by accepting SparkSession as a parameter.

**Spark configuration for tests:**

```scala
SparkSession.builder()
  .master("local[*]")
  .appName("spark-sql-coverage-test")
  .config("spark.ui.enabled", "false")        // No Spark UI in tests
  .config("spark.sql.shuffle.partitions", "1") // Minimize shuffle overhead
  .getOrCreate()
```

- `local[*]` uses all available cores for test parallelism.
- `spark.ui.enabled = false` avoids port conflicts when multiple test suites run.
- `spark.sql.shuffle.partitions = 1` keeps shuffle operations cheap for small test data.

### 3.5 Integration: End-to-End Data Pipeline

The Phase 1 data pipeline composes all components:

```
.sql files ──► SqlFileParser.parse() ──► Seq[SqlStatement]
                                                │
config.yaml ──► DataSourceConfig.resolve() ──► Map[String, Path]
                                                │
                                                ▼
                                     DataLoader.loadAll(dataSources, spark)
                                                │
                                                ▼
                                     Spark temp views registered
                                                │
                                                ▼
                                     SqlExecutor.executeAll(statements, spark)
                                                │
                                                ▼
                                     Seq[ExecutionResult]
```

This full pipeline will be exercised in the integration test (`DataPipelineIntegrationSpec`).

## 4. Test Strategy

### 4.1 Shared Spark Test Trait

All Spark-dependent test suites will mix in a shared trait to avoid duplicating SparkSession boilerplate:

```scala
package com.bob.sparkcoverage

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>
  @transient lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("spark-sql-coverage-test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.catalog.listTables().collect().foreach { t =>
      spark.catalog.dropTempView(t.name)
    }
    super.afterAll()
  }
}
```

**Note:** The SparkSession is **not stopped** in `afterAll`. SparkSession is a JVM-wide singleton in local mode — stopping it would break other test suites running in the same Maven JVM. Instead, we drop temp views to avoid cross-test contamination.

**File:** `src/test/scala/com/bob/sparkcoverage/SharedSparkSession.scala`

### 4.2 DataLoaderSpec (Unit Tests)

**File:** `src/test/scala/com/bob/sparkcoverage/engine/DataLoaderSpec.scala`

Tests use `SharedSparkSession` trait and temp files/directories.

| Test | What it verifies |
|---|---|
| Load single CSV into temp view | DataFrame registered, correct row count, correct schema columns |
| Load multiple CSVs | All temp views registered with correct names |
| CSV with inferred types | Numeric columns inferred as IntegerType/DoubleType, not String |
| Empty CSV (header only, no data) | View registered with 0 rows |
| Missing CSV file | `LoadResult(success=false)` with error message; no exception thrown |
| Malformed CSV | Loads with nulls in permissive mode; `LoadResult(success=true)` |
| Verify temp view is queryable | After loading, `spark.sql("SELECT * FROM tableName")` works |

### 4.3 SqlExecutorSpec (Unit Tests)

**File:** `src/test/scala/com/bob/sparkcoverage/engine/SqlExecutorSpec.scala`

Tests use `SharedSparkSession` trait. Setup loads known CSVs into temp views before each test.

| Test | What it verifies |
|---|---|
| Execute simple SELECT | `ExecutionResult(status=Success, rowCount=Some(n), schema=Some(...))` |
| Execute SELECT with WHERE | Returns filtered row count |
| Execute SELECT with JOIN | Joins across loaded tables, correct row count |
| DDL statement is skipped | `CREATE TABLE ...` → `ExecutionResult(status=Skipped)` |
| Multiple DDL keywords skipped | `DROP`, `ALTER`, `TRUNCATE`, `SET`, `USE` all return Skipped |
| Syntax error recovery | Invalid SQL → `ExecutionResult(status=Error, error=...)` |
| Missing table recovery | `SELECT * FROM nonexistent` → `ExecutionResult(status=Error)` |
| Multiple statements mixed | DDL + valid + error → correct status for each, all processed |
| Empty result set | `SELECT ... WHERE false_condition` → `Success, rowCount=Some(0)` |
| WITH (CTE) queries | `WITH cte AS (...) SELECT ...` → classified as DML, executed |
| `isDdl` classification | Direct tests of the classifier for all keyword categories |

### 4.4 DataPipelineIntegrationSpec (Integration Tests)

**File:** `src/test/scala/com/bob/sparkcoverage/integration/DataPipelineIntegrationSpec.scala`

End-to-end tests composing all Phase 1 components. Uses `SharedSparkSession` and test resource files.

| Test | What it verifies |
|---|---|
| Parse SQL + load CSVs + execute | Full pipeline: parse `multi_statement.sql`, resolve data sources from config YAML, load CSVs, execute all statements, verify each produces `Success` with expected row counts |
| Convention-based data loading | No YAML config — tables resolved by convention, loaded, queries execute |
| Mixed DDL and DML file | Parse file with DDL + SELECT. DDL statements skipped, SELECT statements executed successfully |
| Partial failure recovery | File has valid statement + statement referencing missing table. Valid statement succeeds, missing-table statement returns Error, pipeline continues |
| Complex query execution | Parse `complex_query.sql` (JOINs, CASE, HAVING), load all required CSVs, execute, verify non-zero row counts |

### 4.5 Test Resource Files

These resources serve unit and integration tests (not acceptance tests, which have their own fixtures under `acceptance/`).

**`src/test/resources/sql/simple_select.sql`**
```sql
SELECT order_id, amount FROM orders WHERE status = 'active';
```

**`src/test/resources/sql/multi_statement.sql`**
```sql
SELECT * FROM orders WHERE amount > 100;
SELECT o.order_id, c.customer_name
FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
SELECT product_name FROM products WHERE active = true;
```

**`src/test/resources/sql/ddl_mixed.sql`**
```sql
CREATE TABLE temp_staging (id INT, name STRING);
SELECT order_id, amount FROM orders WHERE status = 'active';
DROP TABLE IF EXISTS temp_staging;
SELECT customer_name FROM customers WHERE region = 'US';
```

**`src/test/resources/sql/syntax_error.sql`**
```sql
SELECT order_id FROM orders;
SELEC broken syntax here;
SELECT customer_name FROM customers;
```

**`src/test/resources/sql/missing_table.sql`**
```sql
SELECT * FROM orders;
SELECT * FROM nonexistent_table;
SELECT * FROM customers;
```

**`src/test/resources/data/orders.csv`, `customers.csv`, `products.csv`** — Standard test data files with headers and sample rows. Additionally:

**`src/test/resources/data/empty.csv`**
```csv
id,name,value
```
(Header only, no data rows.)

**`src/test/resources/data/malformed.csv`**
```csv
id,name,value
1,Alice,100
2,Bob
3,Charlie,300,extra_field
```
(Inconsistent column counts.)

## 5. Error Handling Strategy

### 5.1 Principle: Fail Narrow, Report Wide

Individual component failures should not cascade. Each layer handles its own errors and produces structured results that the caller can inspect.

| Layer | Error | Handling |
|---|---|---|
| `SqlFileParser` | File not found | Throws `IOException` — caller decides whether to continue with other files |
| `SqlFileParser` | Empty file | Returns empty `Seq[SqlStatement]` — not an error |
| `DataSourceConfig` | Missing config file | `configFile = None` triggers pure convention-based resolution |
| `DataSourceConfig` | Table not in config | Falls back to convention (`tableName.csv` in data dir) |
| `DataLoader` | CSV file not found | `LoadResult(success=false)` — does not throw, continues loading other tables |
| `DataLoader` | CSV parse failure | Spark's permissive mode absorbs most issues; `LoadResult(success=true)` with nulls |
| `SqlExecutor` | DDL statement | `ExecutionResult(status=Skipped)` — not an error |
| `SqlExecutor` | SQL syntax error | Catches `ParseException` → `ExecutionResult(status=Error)` — continues |
| `SqlExecutor` | Missing table | Catches `AnalysisException` → `ExecutionResult(status=Error)` — continues |
| `SqlExecutor` | Runtime exception | Catches all `Exception` subtypes → `ExecutionResult(status=Error)` — continues |

### 5.2 Logging

Phase 1 uses `println`-based logging (stderr) for warnings and errors. A structured logging framework (e.g., SLF4J via Spark's bundled Log4j) is deferred to a later phase when the CLI entry point is built.

Logged events:
- `[WARN] Skipping DDL statement: CREATE TABLE ... (file.sql:3)`
- `[ERROR] Failed to execute statement (file.sql:5): Table or view not found: nonexistent_table`
- `[WARN] Failed to load CSV for table 'orders': File not found: /path/to/orders.csv`

## 6. Dependencies and pom.xml

Phase 1 requires creating the `pom.xml` with the following dependencies:

| Dependency | Scope | Purpose |
|---|---|---|
| `scala-library` (2.13.x) | compile | Scala standard library |
| `spark-sql` (3.5.x) | provided + test | SparkSession, DataFrame, CSV reader |
| `spark-catalyst` (3.5.x) | provided | Available transitively through spark-sql |
| `scalatest` (3.x) | test | Test framework |
| `jackson-dataformat-yaml` | compile | YAML config file parsing (bundled with Spark at runtime, needed for compile) |

The `pom.xml` must also configure:
- `scala-maven-plugin` for Scala compilation
- `maven-shade-plugin` for fat JAR packaging
- `scalatest-maven-plugin` for running ScalaTest suites
- `owasp dependency-check-maven` and `spotbugs-maven-plugin` for quality checks (per CLAUDE.md)

The `spark-sql` dependency at test scope provides the runtime Spark needed for `DataLoader` and `SqlExecutor` tests.

## 7. Implementation Order

1. **Create `pom.xml`** with all dependencies and plugins
2. **Create data models** — `ExpressionType.scala`, `CoverageModels.scala` (including `ExecutionResult`, `ExecutionStatus`)
3. **Implement `SqlFileParser`** with unit tests (`SqlFileParserSpec`)
4. **Implement `DataSourceConfig`** with unit tests (`DataSourceConfigSpec`)
5. **Create `SharedSparkSession`** test trait
6. **Create test resource files** (SQL fixtures, CSV fixtures)
7. **Implement `DataLoader`** with unit tests (`DataLoaderSpec`)
8. **Implement `SqlExecutor`** with unit tests (`SqlExecutorSpec`)
9. **Write acceptance tests** — `CoverageModelsAcceptanceSpec`, `DataSourceConfigAcceptanceSpec`, `SqlFileParserAcceptanceSpec`, `Phase1IntegrationAcceptanceSpec`
10. **Write `DataPipelineIntegrationSpec`** — integration tests composing all components
11. **Verify all tests pass** — `mvn test` including acceptance tests

Each step should produce a passing test suite before moving to the next.

## 8. What Phase 1 Does NOT Include

These are explicitly out of scope and belong to later phases:

- **Expression extraction** — Walking Catalyst ASTs to find coverable expressions (Phase 2)
- **Check query generation** — Building `SUM(CASE WHEN ...)` queries (Phase 2)
- **CoverageEngine trait** — The engine abstraction over the full pipeline (Phase 2)
- **BasicCoverageEngine** — The engine that orchestrates extraction + check queries + execution (Phase 2)
- **Reporters** — CLI/HTML/JSON output (Phase 3)
- **Lineage tracing** — Column-level lineage extraction (Phase 5)
- **CLI entry point** — Argument parsing and main method (Future)
- **SparkSession lifecycle management** — The engine will own session creation; Phase 1 tests create their own

## 9. Open Questions

1. **Schema representation** — The LLD uses `Seq[String]` (column names only) for `ExecutionResult.schema`. Should we also capture column types (as strings like `"IntegerType"`, `"StringType"`)? This would be useful for future data-type-aware coverage analysis but adds complexity. **Recommendation:** Start with column names only; add types if a future phase needs them.

2. **Row count performance** — `df.count()` triggers a full table scan on the result DataFrame. For large test datasets this could be slow. An alternative is `df.isEmpty` (just checks for any rows) plus `df.take(1)` for schema. **Recommendation:** Use `count()` — Phase 1 targets small test datasets. Optimize later if performance is a concern.

3. **Duplicate temp view names** — If two different CSV files map to the same table name (e.g., via YAML config error), `createOrReplaceTempView` silently replaces. Should we detect and warn? **Recommendation:** No — the `DataSourceConfig` already produces a `Map[String, Path]` which deduplicates by key. If the user maps two entries to the same table name in YAML, last-write-wins is acceptable.
