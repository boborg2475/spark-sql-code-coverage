package com.bob.sparkcoverage.acceptance

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import com.bob.sparkcoverage.engine.ExpressionExtractor
import com.bob.sparkcoverage.model.ExpressionType._
import com.bob.sparkcoverage.model.CoverageModels._

class ExpressionExtractorAcceptanceSpec extends AnyFunSuite with Matchers {

  private def extractFromSql(sql: String): Seq[CoverableExpression] = {
    val plan = CatalystSqlParser.parsePlan(sql)
    ExpressionExtractor.extract(plan, sourceFile = "test.sql", lineNumber = 1)
  }

  // --- CASE expressions ---

  test("extract CASE WHEN branches as CASE_BRANCH expressions") {
    val sql =
      """SELECT order_id,
        |  CASE
        |    WHEN status = 'active' THEN 'Active'
        |    WHEN status = 'pending' THEN 'Pending'
        |    WHEN status = 'cancelled' THEN 'Cancelled'
        |    ELSE 'Unknown'
        |  END AS status_label
        |FROM orders""".stripMargin

    val expressions = extractFromSql(sql)
    val caseBranches = expressions.filter(_.expressionType == CASE_BRANCH)
    val caseElse = expressions.filter(_.expressionType == CASE_ELSE)

    caseBranches should have size 3
    caseElse should have size 1

    // Each branch should have a SQL fragment referencing the condition
    caseBranches.exists(_.sqlFragment.contains("active")) shouldBe true
    caseBranches.exists(_.sqlFragment.contains("pending")) shouldBe true
    caseBranches.exists(_.sqlFragment.contains("cancelled")) shouldBe true
  }

  test("extract CASE ELSE as CASE_ELSE expression") {
    val sql =
      """SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END AS label
        |FROM data""".stripMargin

    val expressions = extractFromSql(sql)
    val caseElse = expressions.filter(_.expressionType == CASE_ELSE)

    caseElse should have size 1
  }

  test("extract CASE without ELSE should not produce CASE_ELSE") {
    val sql =
      """SELECT CASE WHEN status = 'active' THEN 'Active' END AS label
        |FROM orders""".stripMargin

    val expressions = extractFromSql(sql)
    val caseElse = expressions.filter(_.expressionType == CASE_ELSE)

    caseElse shouldBe empty
  }

  // --- WHERE predicates ---

  test("extract WHERE clause predicates as WHERE_PREDICATE expressions") {
    val sql = "SELECT order_id FROM orders WHERE status = 'active' AND amount > 100"

    val expressions = extractFromSql(sql)
    val wherePredicates = expressions.filter(_.expressionType == WHERE_PREDICATE)

    // Should decompose compound AND into individual predicates
    wherePredicates.size should be >= 2
    wherePredicates.exists(_.sqlFragment.contains("active")) shouldBe true
    wherePredicates.exists(_.sqlFragment.contains("amount")) shouldBe true
  }

  test("extract WHERE with OR produces predicates for each side") {
    val sql = "SELECT order_id FROM orders WHERE status = 'active' OR amount > 1000"

    val expressions = extractFromSql(sql)
    val wherePredicates = expressions.filter(_.expressionType == WHERE_PREDICATE)

    wherePredicates.size should be >= 2
  }

  // --- JOIN conditions ---

  test("extract JOIN ON condition as JOIN_CONDITION expression") {
    val sql =
      """SELECT o.order_id, c.customer_name
        |FROM orders o
        |JOIN customers c ON o.customer_id = c.customer_id""".stripMargin

    val expressions = extractFromSql(sql)
    val joinConditions = expressions.filter(_.expressionType == JOIN_CONDITION)

    joinConditions should have size 1
    joinConditions.head.sqlFragment should include("customer_id")
  }

  test("extract multiple JOIN conditions from multi-join query") {
    val sql =
      """SELECT o.order_id, c.customer_name, p.product_name
        |FROM orders o
        |JOIN customers c ON o.customer_id = c.customer_id
        |JOIN products p ON o.product_id = p.product_id""".stripMargin

    val expressions = extractFromSql(sql)
    val joinConditions = expressions.filter(_.expressionType == JOIN_CONDITION)

    joinConditions should have size 2
  }

  // --- HAVING predicates ---

  test("extract HAVING clause predicates as HAVING_PREDICATE expressions") {
    val sql =
      """SELECT customer_id, COUNT(*) AS cnt
        |FROM orders
        |GROUP BY customer_id
        |HAVING COUNT(*) > 5""".stripMargin

    val expressions = extractFromSql(sql)
    val havingPredicates = expressions.filter(_.expressionType == HAVING_PREDICATE)

    havingPredicates should have size 1
  }

  test("extract compound HAVING clause into multiple predicates") {
    val sql =
      """SELECT customer_id, COUNT(*) AS cnt, SUM(amount) AS total
        |FROM orders
        |GROUP BY customer_id
        |HAVING COUNT(*) > 5 AND SUM(amount) > 1000""".stripMargin

    val expressions = extractFromSql(sql)
    val havingPredicates = expressions.filter(_.expressionType == HAVING_PREDICATE)

    havingPredicates.size should be >= 2
  }

  // --- COALESCE ---

  test("extract COALESCE arguments as COALESCE_BRANCH expressions") {
    val sql = "SELECT COALESCE(discount, rebate, 0) AS effective_discount FROM orders"

    val expressions = extractFromSql(sql)
    val coalesceBranches = expressions.filter(_.expressionType == COALESCE_BRANCH)

    // COALESCE with 3 arguments should produce 3 branches
    coalesceBranches should have size 3
  }

  // --- IF ---

  test("extract IF expression as IF_BRANCH expressions") {
    val sql = "SELECT IF(amount > 500, 'high', 'low') AS tier FROM orders"

    val expressions = extractFromSql(sql)
    val ifBranches = expressions.filter(_.expressionType == IF_BRANCH)

    // IF has a true branch and a false branch
    ifBranches should have size 2
  }

  // --- NULLIF ---

  test("extract NULLIF expression as NULLIF_BRANCH expression") {
    val sql = "SELECT NULLIF(status, 'cancelled') AS active_status FROM orders"

    val expressions = extractFromSql(sql)
    val nullifBranches = expressions.filter(_.expressionType == NULLIF_BRANCH)

    nullifBranches should have size 1
  }

  // --- Expression metadata ---

  test("every extracted expression has a unique id") {
    val sql =
      """SELECT
        |  CASE WHEN status = 'active' THEN 1 WHEN status = 'pending' THEN 2 ELSE 3 END AS code,
        |  IF(amount > 100, 'high', 'low') AS tier
        |FROM orders
        |WHERE region = 'US'""".stripMargin

    val expressions = extractFromSql(sql)
    expressions should not be empty

    val ids = expressions.map(_.id)
    ids.distinct should have size ids.size // all unique
  }

  test("every extracted expression carries source file and line number") {
    val sql = "SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END AS sign FROM data"

    val expressions = extractFromSql(sql)
    expressions should not be empty

    expressions.foreach { expr =>
      expr.sourceFile shouldBe "test.sql"
      expr.lineNumber should be >= 1
    }
  }

  // --- Mixed expressions in one query ---

  test("extract multiple expression types from a single complex query") {
    val sql =
      """SELECT
        |  o.order_id,
        |  CASE WHEN o.amount > 1000 THEN 'Premium' ELSE 'Standard' END AS tier,
        |  COALESCE(o.discount, 0) AS discount,
        |  IF(c.region = 'US', 'Domestic', 'International') AS market
        |FROM orders o
        |JOIN customers c ON o.customer_id = c.customer_id
        |WHERE o.status = 'active' AND o.amount > 50""".stripMargin

    val expressions = extractFromSql(sql)

    val caseBranches = expressions.filter(_.expressionType == CASE_BRANCH)
    val caseElse = expressions.filter(_.expressionType == CASE_ELSE)
    val wherePredicates = expressions.filter(_.expressionType == WHERE_PREDICATE)
    val joinConditions = expressions.filter(_.expressionType == JOIN_CONDITION)
    val coalesceBranches = expressions.filter(_.expressionType == COALESCE_BRANCH)
    val ifBranches = expressions.filter(_.expressionType == IF_BRANCH)

    caseBranches should not be empty
    caseElse should not be empty
    wherePredicates should not be empty
    joinConditions should not be empty
    coalesceBranches should not be empty
    ifBranches should not be empty
  }

  // --- Edge cases ---

  test("handle simple SELECT with no coverable expressions") {
    val sql = "SELECT order_id, amount FROM orders"

    val expressions = extractFromSql(sql)
    expressions shouldBe empty
  }

  test("handle nested CASE inside CASE") {
    val sql =
      """SELECT
        |  CASE
        |    WHEN category = 'electronics' THEN
        |      CASE WHEN price > 500 THEN 'expensive' ELSE 'affordable' END
        |    ELSE 'other'
        |  END AS label
        |FROM products""".stripMargin

    val expressions = extractFromSql(sql)
    val caseBranches = expressions.filter(_.expressionType == CASE_BRANCH)
    val caseElse = expressions.filter(_.expressionType == CASE_ELSE)

    // Outer CASE: 1 branch + 1 else; Inner CASE: 1 branch + 1 else
    caseBranches.size should be >= 2
    caseElse.size should be >= 2
  }
}
