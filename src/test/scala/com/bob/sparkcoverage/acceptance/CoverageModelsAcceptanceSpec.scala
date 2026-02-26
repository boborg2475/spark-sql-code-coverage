package com.bob.sparkcoverage.acceptance

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

import com.bob.sparkcoverage.model.ExpressionType
import com.bob.sparkcoverage.model.ExpressionType._
import com.bob.sparkcoverage.model.CoverageModels._

class CoverageModelsAcceptanceSpec extends AnyFunSuite with Matchers {

  // --- ExpressionType sealed trait ---

  test("ExpressionType should define all 10 expression types") {
    val allTypes: Set[ExpressionType] = Set(
      CASE_BRANCH,
      CASE_ELSE,
      WHERE_PREDICATE,
      JOIN_CONDITION,
      HAVING_PREDICATE,
      COALESCE_BRANCH,
      IF_BRANCH,
      NULLIF_BRANCH,
      SUBQUERY_PREDICATE,
      FILTER_PREDICATE
    )

    allTypes should have size 10
  }

  // --- SqlStatement ---

  test("SqlStatement should carry sql, sourceFile, lineNumber, and statementIdx") {
    val stmt = SqlStatement(
      sql = "SELECT * FROM orders WHERE status = 'active'",
      sourceFile = "/path/to/orders.sql",
      lineNumber = 1,
      statementIdx = 0
    )

    stmt.sql should include("SELECT")
    stmt.sourceFile shouldBe "/path/to/orders.sql"
    stmt.lineNumber shouldBe 1
    stmt.statementIdx shouldBe 0
  }

  // --- Lineage models ---

  test("lineage models should be instantiable with correct fields") {
    val colRef = ColumnRef(table = Some("orders"), column = "amount")
    colRef.table shouldBe Some("orders")
    colRef.column shouldBe "amount"

    val colRefNoTable = ColumnRef(table = None, column = "calculated_field")
    colRefNoTable.table shouldBe None

    val edge = LineageEdge(
      output = ColumnRef(Some("result"), "total"),
      inputs = Set(ColumnRef(Some("orders"), "amount"), ColumnRef(Some("orders"), "discount")),
      transformationSql = Some("amount - discount")
    )
    edge.inputs should have size 2
    edge.transformationSql shouldBe Some("amount - discount")

    val graph = LineageGraph(
      edges = Seq(edge),
      expressionColumns = Map("expr_1" -> Set(colRef))
    )
    graph.edges should have size 1
    graph.expressionColumns should contain key "expr_1"
  }

  // --- CoverableExpression ---

  test("CoverableExpression should carry all fields including lineage columns") {
    val lineageCols = Set(
      ColumnRef(Some("orders"), "status"),
      ColumnRef(Some("orders"), "amount")
    )

    val expr = CoverableExpression(
      id = "expr_001",
      expressionType = CASE_BRANCH,
      context = "CASE WHEN status = 'active'",
      sqlFragment = "status = 'active'",
      sourceFile = "/path/to/query.sql",
      lineNumber = 5,
      lineageColumns = lineageCols
    )

    expr.id shouldBe "expr_001"
    expr.expressionType shouldBe CASE_BRANCH
    expr.context should include("CASE WHEN")
    expr.sqlFragment shouldBe "status = 'active'"
    expr.sourceFile shouldBe "/path/to/query.sql"
    expr.lineNumber shouldBe 5
    expr.lineageColumns should have size 2
    expr.lineageColumns should contain(ColumnRef(Some("orders"), "status"))
  }

  // --- Coverage hierarchy ---

  test("coverage result hierarchy: CoverageResult -> FileCoverage -> StatementCoverage -> ExpressionCoverage") {
    val expr = CoverableExpression(
      id = "expr_1",
      expressionType = WHERE_PREDICATE,
      context = "WHERE amount > 50",
      sqlFragment = "amount > 50",
      sourceFile = "test.sql",
      lineNumber = 3,
      lineageColumns = Set(ColumnRef(Some("orders"), "amount"))
    )

    val exprCoverage = ExpressionCoverage(
      expression = expr,
      isCovered = true,
      checkQuery = "SELECT COUNT(*) FROM orders WHERE amount > 50"
    )
    exprCoverage.isCovered shouldBe true
    exprCoverage.expression.expressionType shouldBe WHERE_PREDICATE

    val stmt = SqlStatement("SELECT * FROM orders WHERE amount > 50", "test.sql", 1, 0)
    val stmtCoverage = StatementCoverage(
      statement = stmt,
      expressions = Seq(exprCoverage),
      coveragePercent = 100.0
    )
    stmtCoverage.expressions should have size 1
    stmtCoverage.coveragePercent shouldBe 100.0

    val fileCoverage = FileCoverage(
      filePath = "test.sql",
      statements = Seq(stmtCoverage),
      coveragePercent = 100.0
    )
    fileCoverage.statements should have size 1

    val lineageGraph = LineageGraph(edges = Seq.empty, expressionColumns = Map.empty)

    val result = CoverageResult(
      files = Seq(fileCoverage),
      byExpressionType = Map(WHERE_PREDICATE -> TypeCoverage(total = 1, covered = 1, coveragePercent = 100.0)),
      lineageGraph = lineageGraph,
      coveragePercent = 100.0
    )

    result.files should have size 1
    result.byExpressionType should contain key WHERE_PREDICATE
    result.coveragePercent shouldBe 100.0
    result.lineageGraph.edges shouldBe empty
  }

  // --- DataSource ---

  test("DataSource should carry tableName, filePath, and format with csv default") {
    val ds = DataSource(
      tableName = "orders",
      filePath = Paths.get("/data/orders.csv"),
      format = "csv"
    )
    ds.tableName shouldBe "orders"
    ds.filePath.toString should endWith("orders.csv")
    ds.format shouldBe "csv"

    // Test default format
    val dsDefault = DataSource(
      tableName = "customers",
      filePath = Paths.get("/data/customers.csv")
    )
    dsDefault.format shouldBe "csv"
  }
}
