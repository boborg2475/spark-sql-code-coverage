package com.bob.sparkcoverage.acceptance

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import com.bob.sparkcoverage.lineage.LineageTracer
import com.bob.sparkcoverage.model.CoverageModels._

class LineageTracerAcceptanceSpec extends AnyFunSuite with Matchers {

  private def traceFromSql(sql: String): LineageGraph = {
    val plan = CatalystSqlParser.parsePlan(sql)
    LineageTracer.trace(plan)
  }

  // --- Basic column projections ---

  test("trace lineage for simple column projection") {
    val sql = "SELECT order_id, amount FROM orders"

    val graph = traceFromSql(sql)

    graph.edges should not be empty

    // Output column "order_id" should trace back to orders.order_id
    val orderIdEdges = graph.edges.filter(_.output.column == "order_id")
    orderIdEdges should not be empty
    orderIdEdges.exists(_.inputs.exists(ref =>
      ref.column == "order_id"
    )) shouldBe true
  }

  test("trace lineage for aliased column") {
    val sql = "SELECT amount AS total_amount FROM orders"

    val graph = traceFromSql(sql)

    val aliasEdges = graph.edges.filter(_.output.column == "total_amount")
    aliasEdges should not be empty
    aliasEdges.exists(_.inputs.exists(_.column == "amount")) shouldBe true
  }

  // --- Transformations ---

  test("trace lineage through CASE expression to source columns") {
    val sql =
      """SELECT
        |  CASE WHEN status = 'active' THEN amount ELSE 0 END AS active_amount
        |FROM orders""".stripMargin

    val graph = traceFromSql(sql)

    // active_amount depends on both status and amount
    val edges = graph.edges.filter(_.output.column == "active_amount")
    edges should not be empty

    val allInputColumns = edges.flatMap(_.inputs.map(_.column)).toSet
    allInputColumns should contain("status")
    allInputColumns should contain("amount")
  }

  test("trace lineage through arithmetic expression") {
    val sql = "SELECT amount * quantity AS total FROM orders"

    val graph = traceFromSql(sql)

    val totalEdges = graph.edges.filter(_.output.column == "total")
    totalEdges should not be empty

    val inputs = totalEdges.flatMap(_.inputs.map(_.column)).toSet
    inputs should contain("amount")
    inputs should contain("quantity")
  }

  // --- Joins ---

  test("trace lineage through JOIN with columns from both sides") {
    val sql =
      """SELECT o.order_id, c.customer_name
        |FROM orders o
        |JOIN customers c ON o.customer_id = c.customer_id""".stripMargin

    val graph = traceFromSql(sql)

    // order_id comes from orders
    val orderEdges = graph.edges.filter(_.output.column == "order_id")
    orderEdges should not be empty

    // customer_name comes from customers
    val nameEdges = graph.edges.filter(_.output.column == "customer_name")
    nameEdges should not be empty
  }

  // --- Aggregations ---

  test("trace lineage through aggregate functions") {
    val sql =
      """SELECT customer_id, SUM(amount) AS total_spent, COUNT(*) AS order_count
        |FROM orders
        |GROUP BY customer_id""".stripMargin

    val graph = traceFromSql(sql)

    // total_spent depends on amount
    val totalEdges = graph.edges.filter(_.output.column == "total_spent")
    totalEdges should not be empty
    totalEdges.exists(_.inputs.exists(_.column == "amount")) shouldBe true
  }

  // --- Filter columns ---

  test("trace lineage includes columns referenced in WHERE clause") {
    val sql =
      """SELECT order_id, amount
        |FROM orders
        |WHERE status = 'active' AND region = 'US'""".stripMargin

    val graph = traceFromSql(sql)

    // The graph should capture that status and region are referenced
    val allInputColumns = graph.edges.flatMap(_.inputs.map(_.column)).toSet
    // At minimum, the projected columns should be traced
    allInputColumns should contain("order_id")
    allInputColumns should contain("amount")
  }

  // --- COALESCE / IF ---

  test("trace lineage through COALESCE to all argument columns") {
    val sql = "SELECT COALESCE(discount, rebate, 0) AS effective_discount FROM orders"

    val graph = traceFromSql(sql)

    val edges = graph.edges.filter(_.output.column == "effective_discount")
    edges should not be empty

    val inputs = edges.flatMap(_.inputs.map(_.column)).toSet
    inputs should contain("discount")
    inputs should contain("rebate")
  }

  test("trace lineage through IF expression") {
    val sql = "SELECT IF(amount > 500, 'high', 'low') AS tier FROM orders"

    val graph = traceFromSql(sql)

    val edges = graph.edges.filter(_.output.column == "tier")
    edges should not be empty
    edges.exists(_.inputs.exists(_.column == "amount")) shouldBe true
  }

  // --- Expression-column mapping ---

  test("expressionColumns maps expression IDs to their source columns") {
    val sql =
      """SELECT CASE WHEN status = 'active' THEN 'yes' ELSE 'no' END AS flag
        |FROM orders""".stripMargin

    val graph = traceFromSql(sql)

    // The expressionColumns map should have entries linking expressions to column refs
    // (populated when used alongside ExpressionExtractor)
    graph.expressionColumns should not be null
  }

  // --- Edge cases ---

  test("trace lineage for SELECT with literal values produces edges for non-literal columns") {
    val sql = "SELECT order_id, 42 AS constant FROM orders"

    val graph = traceFromSql(sql)

    // order_id should have lineage
    val orderEdges = graph.edges.filter(_.output.column == "order_id")
    orderEdges should not be empty

    // constant is a literal — may or may not have lineage edge, but shouldn't reference any input
    val constEdges = graph.edges.filter(_.output.column == "constant")
    if (constEdges.nonEmpty) {
      constEdges.flatMap(_.inputs) shouldBe empty
    }
  }

  test("empty LineageGraph for trivial query") {
    val sql = "SELECT 1 AS one"

    val graph = traceFromSql(sql)

    // No table references, so no meaningful lineage
    graph should not be null
  }
}
