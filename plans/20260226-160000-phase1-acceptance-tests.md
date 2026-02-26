# Phase 1 Acceptance Tests

## Goal

Write acceptance tests that validate Phase 1's deliverable: "Can read .sql files into `Seq[SqlStatement]` and resolve table-to-CSV mappings." These tests are written before the implementation (TDD-style) and serve as the specification that the Phase 1 implementing agent must satisfy.

Per CLAUDE.md, acceptance tests must be written separately from the implementing agent.

## Affected Files

### New files (test resources)

- `src/test/resources/acceptance/sql/multi_statement.sql`
- `src/test/resources/acceptance/sql/with_comments.sql`
- `src/test/resources/acceptance/sql/string_with_semicolons.sql`
- `src/test/resources/acceptance/sql/single_statement.sql`
- `src/test/resources/acceptance/sql/empty_file.sql`
- `src/test/resources/acceptance/sql/complex_query.sql`
- `src/test/resources/acceptance/data/orders.csv`
- `src/test/resources/acceptance/data/customers.csv`
- `src/test/resources/acceptance/data/products.csv`
- `src/test/resources/acceptance/config/coverage-config.yaml`
- `src/test/resources/acceptance/config/partial-config.yaml`

### New files (test classes)

- `src/test/scala/com/bob/sparkcoverage/acceptance/SqlFileParserAcceptanceSpec.scala`
- `src/test/scala/com/bob/sparkcoverage/acceptance/DataSourceConfigAcceptanceSpec.scala`
- `src/test/scala/com/bob/sparkcoverage/acceptance/CoverageModelsAcceptanceSpec.scala`
- `src/test/scala/com/bob/sparkcoverage/acceptance/Phase1IntegrationAcceptanceSpec.scala`

### New files (build)

- `pom.xml` — Minimal Maven build for Scala 2.13 + Spark 3.5.x + ScalaTest

## Approach

1. Create a minimal `pom.xml` to bootstrap the project (required for compilation).
2. Create realistic test resource files (SQL, CSV, YAML) that exercise edge cases.
3. Write 4 acceptance test classes covering:
   - SQL file parsing (9 test cases)
   - Data source config resolution (8 test cases)
   - Data model completeness (6 test cases)
   - End-to-end integration of parser + config (4 test cases)

## Testing

These ARE the tests. They will not compile until Phase 1 source code is implemented. Once Phase 1 is done:

```bash
mvn test -Dsuites="com.bob.sparkcoverage.acceptance.SqlFileParserAcceptanceSpec,com.bob.sparkcoverage.acceptance.DataSourceConfigAcceptanceSpec,com.bob.sparkcoverage.acceptance.CoverageModelsAcceptanceSpec,com.bob.sparkcoverage.acceptance.Phase1IntegrationAcceptanceSpec"
```

## Open Questions

None — the implementation plan provides clear API signatures and data models.
