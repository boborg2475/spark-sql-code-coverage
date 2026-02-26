# Phase 2 Acceptance Tests

## Goal

Write acceptance tests that validate Phase 2's deliverable: "Given SQL, produce `Seq[CoverableExpression]` with lineage metadata." These tests validate the `ExpressionExtractor` and `LineageTracer` components end-to-end.

Phase 2 components:
- `ExpressionExtractor` — walks Catalyst LogicalPlan/Expression trees to identify coverable expressions
- `LineageTracer` — extracts column-level lineage alongside expressions
- Each `CoverableExpression` carries `lineageColumns: Set[ColumnRef]` from creation

## Affected Files

### New test resource files

- `src/test/resources/acceptance/sql/case_expressions.sql` — CASE WHEN with multiple branches + ELSE
- `src/test/resources/acceptance/sql/where_predicates.sql` — WHERE with compound AND/OR predicates
- `src/test/resources/acceptance/sql/join_conditions.sql` — JOIN ON conditions
- `src/test/resources/acceptance/sql/having_clause.sql` — GROUP BY + HAVING
- `src/test/resources/acceptance/sql/coalesce_if_nullif.sql` — COALESCE, IF, NULLIF expressions
- `src/test/resources/acceptance/sql/mixed_expressions.sql` — Realistic query combining many types

### New test classes

- `src/test/scala/com/bob/sparkcoverage/acceptance/ExpressionExtractorAcceptanceSpec.scala`
- `src/test/scala/com/bob/sparkcoverage/acceptance/LineageTracerAcceptanceSpec.scala`
- `src/test/scala/com/bob/sparkcoverage/acceptance/Phase2IntegrationAcceptanceSpec.scala`

## Approach

Tests use CatalystSqlParser to parse SQL into LogicalPlan, then pass to ExpressionExtractor/LineageTracer. This mirrors how the engine components are actually used.

## Testing

After Phase 2 is implemented:
```bash
mvn test -Dsuites="com.bob.sparkcoverage.acceptance.ExpressionExtractorAcceptanceSpec,com.bob.sparkcoverage.acceptance.LineageTracerAcceptanceSpec,com.bob.sparkcoverage.acceptance.Phase2IntegrationAcceptanceSpec"
```

## Open Questions

None.
