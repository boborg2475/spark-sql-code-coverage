package com.bob.sparkcoverage.acceptance

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Path, Paths}

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import com.bob.sparkcoverage.parser.SqlFileParser
import com.bob.sparkcoverage.engine.ExpressionExtractor
import com.bob.sparkcoverage.lineage.LineageTracer
import com.bob.sparkcoverage.model.ExpressionType._
import com.bob.sparkcoverage.model.CoverageModels._

class Phase2IntegrationAcceptanceSpec extends AnyFunSuite with Matchers {

  private def resourcePath(name: String): Path = {
    val url = getClass.getClassLoader.getResource(s"acceptance/sql/$name")
    require(url != null, s"Test resource not found: acceptance/sql/$name")
    Paths.get(url.toURI)
  }

  // --- End-to-end: parse SQL file → extract expressions with lineage ---

  test("parse SQL file, extract coverable expressions, and verify lineage columns are populated") {
    val statements = SqlFileParser.parse(resourcePath("case_expressions.sql"))
    statements should have size 1

    val plan = CatalystSqlParser.parsePlan(statements.head.sql)
    val expressions = ExpressionExtractor.extract(plan, statements.head.sourceFile, statements.head.lineNumber)

    expressions should not be empty

    val caseBranches = expressions.filter(_.expressionType == CASE_BRANCH)
    caseBranches should have size 3

    // Each CASE branch should have lineage columns referencing the "status" column
    caseBranches.foreach { expr =>
      expr.lineageColumns should not be empty
      expr.lineageColumns.exists(_.column == "status") shouldBe true
    }
  }

  test("parse complex SQL file and extract all expression types with lineage") {
    val statements = SqlFileParser.parse(resourcePath("mixed_expressions.sql"))
    statements should have size 1

    val plan = CatalystSqlParser.parsePlan(statements.head.sql)
    val expressions = ExpressionExtractor.extract(plan, statements.head.sourceFile, statements.head.lineNumber)
    val lineageGraph = LineageTracer.trace(plan)

    // Should find multiple expression types
    val typeSet = expressions.map(_.expressionType).toSet
    typeSet should contain(CASE_BRANCH)
    typeSet should contain(JOIN_CONDITION)
    typeSet should contain(WHERE_PREDICATE)

    // Lineage graph should have edges
    lineageGraph.edges should not be empty

    // All expressions should have non-empty lineage columns
    expressions.foreach { expr =>
      expr.lineageColumns should not be empty
    }
  }

  test("parse multi-statement SQL file and extract expressions from each statement independently") {
    val statements = SqlFileParser.parse(resourcePath("multi_statement.sql"))
    statements.size should be >= 2

    val allExpressions = statements.flatMap { stmt =>
      val plan = CatalystSqlParser.parsePlan(stmt.sql)
      ExpressionExtractor.extract(plan, stmt.sourceFile, stmt.lineNumber)
    }

    allExpressions should not be empty

    // Expressions from different statements should have different source line numbers
    val lineNumbers = allExpressions.map(_.lineNumber).distinct
    lineNumbers.size should be >= 2
  }

  test("expression extraction and lineage tracing produce consistent column references") {
    val sql =
      """SELECT
        |  CASE WHEN status = 'active' THEN amount ELSE 0 END AS active_amount
        |FROM orders
        |WHERE region = 'US'""".stripMargin

    val plan = CatalystSqlParser.parsePlan(sql)
    val expressions = ExpressionExtractor.extract(plan, "test.sql", 1)
    val lineageGraph = LineageTracer.trace(plan)

    // The CASE expression depends on status and amount
    val caseBranch = expressions.find(_.expressionType == CASE_BRANCH)
    caseBranch shouldBe defined
    val branchColumns = caseBranch.get.lineageColumns.map(_.column)
    branchColumns should contain("status")

    // The lineage graph should also show status flowing into active_amount
    val activeAmountEdges = lineageGraph.edges.filter(_.output.column == "active_amount")
    activeAmountEdges should not be empty
    val lineageInputCols = activeAmountEdges.flatMap(_.inputs.map(_.column)).toSet
    lineageInputCols should contain("status")
    lineageInputCols should contain("amount")
  }

  test("parse WHERE predicates file and extract decomposed predicates with lineage") {
    val statements = SqlFileParser.parse(resourcePath("where_predicates.sql"))
    statements should have size 1

    val plan = CatalystSqlParser.parsePlan(statements.head.sql)
    val expressions = ExpressionExtractor.extract(plan, statements.head.sourceFile, statements.head.lineNumber)

    val wherePredicates = expressions.filter(_.expressionType == WHERE_PREDICATE)
    wherePredicates.size should be >= 2

    // Each predicate should reference the column it tests
    val allLineageCols = wherePredicates.flatMap(_.lineageColumns.map(_.column)).toSet
    allLineageCols should contain("status")
    allLineageCols should contain("amount")
    allLineageCols should contain("region")
  }

  test("parse HAVING clause file and extract HAVING predicates") {
    val statements = SqlFileParser.parse(resourcePath("having_clause.sql"))
    statements should have size 1

    val plan = CatalystSqlParser.parsePlan(statements.head.sql)
    val expressions = ExpressionExtractor.extract(plan, statements.head.sourceFile, statements.head.lineNumber)

    val havingPredicates = expressions.filter(_.expressionType == HAVING_PREDICATE)
    havingPredicates should not be empty
  }

  test("parse COALESCE/IF/NULLIF file and extract all expression types with lineage") {
    val statements = SqlFileParser.parse(resourcePath("coalesce_if_nullif.sql"))
    statements should have size 1

    val plan = CatalystSqlParser.parsePlan(statements.head.sql)
    val expressions = ExpressionExtractor.extract(plan, statements.head.sourceFile, statements.head.lineNumber)

    val coalesce = expressions.filter(_.expressionType == COALESCE_BRANCH)
    val ifBranches = expressions.filter(_.expressionType == IF_BRANCH)
    val nullif = expressions.filter(_.expressionType == NULLIF_BRANCH)

    coalesce should have size 3 // 3 arguments to COALESCE
    ifBranches should have size 2 // true + false branch
    nullif should have size 1

    // COALESCE lineage should include discount and rebate columns
    val coalesceCols = coalesce.flatMap(_.lineageColumns.map(_.column)).toSet
    coalesceCols should contain("discount")
    coalesceCols should contain("rebate")

    // IF lineage should include amount
    val ifCols = ifBranches.flatMap(_.lineageColumns.map(_.column)).toSet
    ifCols should contain("amount")

    // NULLIF lineage should include status
    val nullifCols = nullif.flatMap(_.lineageColumns.map(_.column)).toSet
    nullifCols should contain("status")
  }
}
