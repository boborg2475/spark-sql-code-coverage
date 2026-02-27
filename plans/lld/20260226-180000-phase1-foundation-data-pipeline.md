# Low-Level Design: Phase 1 — Foundation & Data Pipeline

## 1. Goal

Prove the core infrastructure works end-to-end: parse SQL files, load CSV test data into Spark tables, execute SQL against them, and capture results with robust error handling. Phase 1 delivers a working data pipeline that later phases build coverage analysis on top of.

## 2. Component Inventory

| Component | File | Purpose |
|---|---|---|
| `ExpressionType` | `model/ExpressionType.scala` | Sealed trait with 10 coverable expression types |
| `CoverageModels` | `model/CoverageModels.scala` | All data model types (SqlStatement, coverage hierarchy, lineage types, DataSource, ExecutionResult ADT, QueryResult) |
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
/** Successful query execution result. */
case class QueryResult(
  rowCount: Long,
  schema: Seq[String]           // Column names from result schema
)

/** Result of processing a single SQL statement. */
sealed trait ExecutionResult {
  def statement: SqlStatement
}
object ExecutionResult {
  /** DDL or non-coverable statement — classified and skipped without execution. */
  case class Skipped(statement: SqlStatement) extends ExecutionResult
  /** DML statement that was attempted. Success holds QueryResult, Failure holds the exception. */
  case class Executed(statement: SqlStatement, result: Try[QueryResult]) extends ExecutionResult
}
```

**Design decisions:**

- `ExecutionResult` is a sealed trait with two cases rather than a case class with a status enum. `Skipped` vs `Executed` is a classification decision, not an error — modeled as distinct types rather than overloading a single class with optional fields.
- `Executed` wraps `Try[QueryResult]` — `Success` for statements that ran, `Failure` preserving the full exception. This mirrors the `DataLoader` pattern and lets callers use standard `Try` combinators (`map`, `recover`, pattern matching).
- `QueryResult` uses `Seq[String]` for schema (column names only) rather than Spark's `StructType` to avoid leaking Spark types into the model layer. This keeps models serializable and testable without a Spark dependency.
- No separate `ExecutionStatus` enum — the type hierarchy and `Try` express all three states (skipped, success, error) without optional fields.

### 3.2 DataLoader

**File:** `src/main/scala/com/bob/sparkcoverage/engine/DataLoader.scala`

**Responsibility:** Given a `Map[String, Path]` (table name to CSV path), load each CSV file into a temporary view with the corresponding table name. Returns a `Try[DataFrame]` per table — `Success` with the registered DataFrame, or `Failure` with the exception encountered.

```scala
package com.bob.sparkcoverage.engine

import java.nio.file.Path
import scala.util.Try
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataLoader(spark: SparkSession) {

  def loadAll(
    dataSources: Map[String, Path]
  ): Map[String, Try[DataFrame]]
}
```

**Algorithm:**

1. For each `(tableName, csvPath)` entry in `dataSources`:
   a. Wrap the entire load operation in a `Try`:
      - Verify the file exists. If not, throw an appropriate exception (e.g., `FileNotFoundException`).
      - Call `spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath.toString)` to read the CSV.
      - Register the DataFrame as a temporary view: `df.createOrReplaceTempView(tableName)`.
      - Return the DataFrame.
   b. The `Try` captures any exception (file not found, read failure, etc.) as a `Failure`.
2. Return a `Map[String, Try[DataFrame]]` — table name to its load result.

**Design decisions:**

- Using `Try[DataFrame]` instead of a custom result type. `Success` gives callers direct access to the DataFrame for downstream operations. `Failure` preserves the full exception with type, message, and stack trace — no information loss.
- The caller decides how to handle failures (log and continue, abort, etc.) rather than the DataLoader making that decision.

**CSV loading options:**

| Option | Value | Rationale |
|---|---|---|
| `header` | `true` | All test CSV files have a header row with column names |
| `inferSchema` | `true` | Automatically detect column types (numeric, string, boolean) from the data. Avoids requiring an explicit schema definition for each table. |

**Error scenarios:**

| Scenario | Behavior |
|---|---|
| CSV file does not exist | `Failure(FileNotFoundException(...))` |
| CSV file is empty (no data rows) | `Success(df)` — view registered with 0 rows |
| CSV file is malformed (inconsistent columns) | Spark's CSV reader tolerates this by default (`PERMISSIVE` mode). Loads with nulls for missing fields. `Success(df)` |
| Duplicate table name | `createOrReplaceTempView` replaces silently. Last mapping wins. |

### 3.3 SqlExecutor

**File:** `src/main/scala/com/bob/sparkcoverage/engine/SqlExecutor.scala`

**Responsibility:** Given a sequence of `SqlStatement`s (with tables already loaded via `DataLoader`), execute each statement and return structured results. Handles DDL classification, per-statement error recovery, and result capture.

```scala
package com.bob.sparkcoverage.engine

import com.bob.sparkcoverage.model.CoverageModels.{ExecutionResult, QueryResult, SqlStatement}
import org.apache.spark.sql.SparkSession

class SqlExecutor(spark: SparkSession) {

  def executeAll(
    statements: Seq[SqlStatement]
  ): Seq[ExecutionResult]

  /** Classify a SQL statement as DDL (skippable) or DML (executable). */
  private[engine] def isDdl(sql: String): Boolean
}
```

**Algorithm:**

1. For each `SqlStatement` in the input sequence:
   a. **Normalize:** Trim the SQL and extract the first keyword (uppercase).
   b. **Classify:** Call `isDdl(sql)`. If DDL, return `ExecutionResult.Skipped(statement)`.
   c. **Execute:** Wrap execution in a `Try`: call `spark.sql(statement.sql)` to get a `DataFrame`, then `df.count()` for row count, and `df.schema.fieldNames` for column names. Construct a `QueryResult`.
   d. Return `ExecutionResult.Executed(statement, result)` — where `result` is `Success(QueryResult(...))` or `Failure(exception)`.
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
| Syntax error in SQL | `spark.sql()` throws `ParseException`. Caught by `Try` → `Executed(stmt, Failure(e))`. Next statement continues. |
| Missing table reference | `spark.sql()` throws `AnalysisException`. Caught by `Try` → `Executed(stmt, Failure(e))`. Next statement continues. |
| Missing column reference | Same — `AnalysisException`, caught by `Try`. |
| DDL statement | `Skipped(statement)` — not executed. |
| Empty SQL string | Should not reach executor (filtered by SqlFileParser), but if it does: skip it. |
| Statement that returns no rows | `Executed(stmt, Success(QueryResult(rowCount=0, ...)))`. |
| Ambiguous column reference | Spark resolves or throws `AnalysisException`. Caught by `Try` if thrown. |

**Why per-statement recovery matters:** A SQL source file may contain a mix of valid and invalid statements (e.g., a DDL preamble, a query referencing a table not in test data, and several valid queries). The executor must not abort the entire file on the first error — it reports each failure and continues, so downstream coverage analysis can still process the valid statements.

### 3.4 SparkSession Management

Phase 1 does not create a SparkSession itself — tests will create `SparkSession.builder().master("local[*]").appName("test").getOrCreate()` in their test setup. `DataLoader` and `SqlExecutor` receive the `SparkSession` via constructor injection.

In Phase 2, the `BasicCoverageEngine` will own SparkSession creation and pass it to the `DataLoader` and `SqlExecutor` it constructs. Phase 1 components are designed to be engine-agnostic through this inversion of control.

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
                              SparkSession
                                  │
                          ┌───────┴───────┐
                          ▼               ▼
                    DataLoader(spark) SqlExecutor(spark)

.sql files ──► SqlFileParser.parse() ──► Seq[SqlStatement]
                                                │
config.yaml ──► DataSourceConfig.resolve() ──► Map[String, Path]
                                                │
                                                ▼
                                     dataLoader.loadAll(dataSources)
                                                │
                                                ▼
                                     Map[String, Try[DataFrame]]
                                                │
                                                ▼
                                     sqlExecutor.executeAll(statements)
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
| Load single CSV into temp view | Returns `Success(df)`, DataFrame registered as temp view, correct row count and schema columns |
| Load multiple CSVs | All entries are `Success`, all temp views registered with correct names |
| CSV with inferred types | Numeric columns inferred as IntegerType/DoubleType, not String |
| Empty CSV (header only, no data) | `Success(df)` — view registered with 0 rows |
| Missing CSV file | Returns `Failure` containing `FileNotFoundException`; no exception thrown from `loadAll` |
| Malformed CSV | `Success(df)` — loads with nulls in permissive mode |
| Verify temp view is queryable | After loading, `spark.sql("SELECT * FROM tableName")` works on the `Success` DataFrame |

### 4.3 SqlExecutorSpec (Unit Tests)

**File:** `src/test/scala/com/bob/sparkcoverage/engine/SqlExecutorSpec.scala`

Tests use `SharedSparkSession` trait. Setup loads known CSVs into temp views before each test.

| Test | What it verifies |
|---|---|
| Execute simple SELECT | `Executed(stmt, Success(QueryResult(n, schema)))` |
| Execute SELECT with WHERE | Returns `Executed` with filtered row count in `QueryResult` |
| Execute SELECT with JOIN | Joins across loaded tables, correct row count in `QueryResult` |
| DDL statement is skipped | `CREATE TABLE ...` → `Skipped(statement)` |
| Multiple DDL keywords skipped | `DROP`, `ALTER`, `TRUNCATE`, `SET`, `USE` all return `Skipped` |
| Syntax error recovery | Invalid SQL → `Executed(stmt, Failure(ParseException))` |
| Missing table recovery | `SELECT * FROM nonexistent` → `Executed(stmt, Failure(AnalysisException))` |
| Multiple statements mixed | DDL + valid + error → correct type for each, all processed |
| Empty result set | `SELECT ... WHERE false_condition` → `Executed(stmt, Success(QueryResult(0, ...)))` |
| WITH (CTE) queries | `WITH cte AS (...) SELECT ...` → classified as DML, returns `Executed` |
| `isDdl` classification | Direct tests of the classifier for all keyword categories |

### 4.4 DataPipelineIntegrationSpec (Integration Tests)

**File:** `src/test/scala/com/bob/sparkcoverage/integration/DataPipelineIntegrationSpec.scala`

End-to-end tests composing all Phase 1 components. Uses `SharedSparkSession` and test resource files.

| Test | What it verifies |
|---|---|
| Parse SQL + load CSVs + execute | Full pipeline: parse `multi_statement.sql`, resolve data sources from config YAML, load CSVs, execute all statements, verify each returns `Executed` with `Success(QueryResult(...))` and expected row counts |
| Convention-based data loading | No YAML config — tables resolved by convention, loaded, queries execute |
| Mixed DDL and DML file | Parse file with DDL + SELECT. DDL statements return `Skipped`, SELECT statements return `Executed` with `Success` |
| Partial failure recovery | File has valid statement + statement referencing missing table. Valid statement returns `Executed(Success(...))`, missing-table statement returns `Executed(Failure(...))`, pipeline continues |
| Complex query execution | Parse `complex_query.sql` (JOINs, CASE, HAVING), load all required CSVs, execute, verify non-zero row counts in `QueryResult` |

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
| `DataLoader` | CSV file not found | `Failure(FileNotFoundException)` — does not throw from `loadAll`, continues loading other tables |
| `DataLoader` | CSV parse failure | Spark's permissive mode absorbs most issues; `Success(df)` with nulls |
| `SqlExecutor` | DDL statement | `Skipped(statement)` — not an error |
| `SqlExecutor` | SQL syntax error | `Executed(stmt, Failure(ParseException))` — continues |
| `SqlExecutor` | Missing table | `Executed(stmt, Failure(AnalysisException))` — continues |
| `SqlExecutor` | Runtime exception | `Executed(stmt, Failure(exception))` — continues |

### 5.2 Logging

Phase 1 uses `println`-based logging (stderr) for warnings and errors. A structured logging framework (e.g., SLF4J via Spark's bundled Log4j) is deferred to a later phase when the CLI entry point is built.

Logged events:
- `[WARN] Skipping DDL statement: CREATE TABLE ... (file.sql:3)`
- `[ERROR] Failed to execute statement (file.sql:5): Table or view not found: nonexistent_table`
- `[WARN] Failed to load CSV for table 'orders': java.io.FileNotFoundException: /path/to/orders.csv`

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
2. **Create data models** — `ExpressionType.scala`, `CoverageModels.scala` (including `ExecutionResult` ADT, `QueryResult`)
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

1. **Schema representation** — The LLD uses `Seq[String]` (column names only) for `QueryResult.schema`. Should we also capture column types (as strings like `"IntegerType"`, `"StringType"`)? This would be useful for future data-type-aware coverage analysis but adds complexity. **Recommendation:** Start with column names only; add types if a future phase needs them.

2. **Row count performance** — `df.count()` triggers a full table scan on the result DataFrame. For large test datasets this could be slow. An alternative is `df.isEmpty` (just checks for any rows) plus `df.take(1)` for schema. **Recommendation:** Use `count()` — Phase 1 targets small test datasets. Optimize later if performance is a concern.

3. **Duplicate temp view names** — If two different CSV files map to the same table name (e.g., via YAML config error), `createOrReplaceTempView` silently replaces. Should we detect and warn? **Recommendation:** No — the `DataSourceConfig` already produces a `Map[String, Path]` which deduplicates by key. If the user maps two entries to the same table name in YAML, last-write-wins is acceptable.
