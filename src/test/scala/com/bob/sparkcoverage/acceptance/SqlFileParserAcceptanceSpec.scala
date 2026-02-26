package com.bob.sparkcoverage.acceptance

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

import com.bob.sparkcoverage.parser.SqlFileParser

class SqlFileParserAcceptanceSpec extends AnyFunSuite with Matchers {

  private def resourcePath(name: String): Path = {
    val url = getClass.getClassLoader.getResource(s"acceptance/sql/$name")
    require(url != null, s"Test resource not found: acceptance/sql/$name")
    Paths.get(url.toURI)
  }

  // --- Multi-statement parsing ---

  test("parse multi-statement SQL file into correct number of statements") {
    val statements = SqlFileParser.parse(resourcePath("multi_statement.sql"))
    statements should have size 3
  }

  test("preserve SQL text for each parsed statement") {
    val statements = SqlFileParser.parse(resourcePath("multi_statement.sql"))

    statements(0).sql should include("CASE")
    statements(0).sql should include("WHEN status = 'active'")
    statements(0).sql should include("FROM orders")

    statements(1).sql should include("JOIN customers")
    statements(1).sql should include("ON o.customer_id = c.customer_id")

    statements(2).sql should include("FROM products")
    statements(2).sql should include("WHEN price > 1000")
  }

  test("track line numbers for each statement") {
    val statements = SqlFileParser.parse(resourcePath("multi_statement.sql"))

    // First statement starts at line 1
    statements(0).lineNumber shouldBe 1

    // Second statement starts after the first (line 12 in the file)
    statements(1).lineNumber should be > statements(0).lineNumber

    // Third statement starts after the second
    statements(2).lineNumber should be > statements(1).lineNumber
  }

  test("assign sequential zero-based statement indices") {
    val statements = SqlFileParser.parse(resourcePath("multi_statement.sql"))

    statements(0).statementIdx shouldBe 0
    statements(1).statementIdx shouldBe 1
    statements(2).statementIdx shouldBe 2
  }

  test("preserve source file path in parsed statements") {
    val path = resourcePath("multi_statement.sql")
    val statements = SqlFileParser.parse(path)

    statements.foreach { stmt =>
      stmt.sourceFile shouldBe path.toString
    }
  }

  // --- Comment handling ---

  test("handle line comments (--) and parse statements correctly") {
    val statements = SqlFileParser.parse(resourcePath("with_comments.sql"))

    statements should have size 2
    // Comments should not appear in parsed SQL
    statements(0).sql should not include "--"
    statements(0).sql should include("FROM orders")
    statements(0).sql should include("WHERE status = 'active'")

    statements(1).sql should include("GROUP BY customer_id")
    statements(1).sql should include("HAVING")
  }

  test("handle block comments (/* */) and parse statements correctly") {
    val statements = SqlFileParser.parse(resourcePath("with_comments.sql"))

    statements should have size 2
    // Block comments should not appear in parsed SQL
    statements.foreach { stmt =>
      stmt.sql should not include "/*"
      stmt.sql should not include "*/"
    }
  }

  // --- Edge cases ---

  test("handle semicolons inside string literals without splitting") {
    val statements = SqlFileParser.parse(resourcePath("string_with_semicolons.sql"))

    // Should be a single statement — the semicolons are inside string literals
    statements should have size 1
    statements(0).sql should include("shipped; delivered")
    statements(0).sql should include("pending; review")
  }

  test("parse single statement without trailing semicolon") {
    val statements = SqlFileParser.parse(resourcePath("single_statement.sql"))

    statements should have size 1
    statements(0).sql should include("FROM customers")
    statements(0).sql should include("WHERE active = true")
    statements(0).statementIdx shouldBe 0
    statements(0).lineNumber shouldBe 1
  }

  test("handle empty file gracefully") {
    val statements = SqlFileParser.parse(resourcePath("empty_file.sql"))
    statements shouldBe empty
  }

  // --- Complex SQL ---

  test("parse complex query with joins and multiple CASE expressions") {
    val statements = SqlFileParser.parse(resourcePath("complex_query.sql"))

    statements should have size 2

    val firstStmt = statements(0)
    firstStmt.sql should include("JOIN customers")
    firstStmt.sql should include("JOIN products")
    firstStmt.sql should include("CASE")
    firstStmt.sql should include("COALESCE")

    val secondStmt = statements(1)
    secondStmt.sql should include("GROUP BY customer_id")
    secondStmt.sql should include("HAVING COUNT(*) > 5")
  }

  // --- Multiple files ---

  test("parse multiple SQL files independently") {
    val multiStmts = SqlFileParser.parse(resourcePath("multi_statement.sql"))
    val singleStmts = SqlFileParser.parse(resourcePath("single_statement.sql"))
    val commentStmts = SqlFileParser.parse(resourcePath("with_comments.sql"))

    multiStmts should have size 3
    singleStmts should have size 1
    commentStmts should have size 2

    // Each file's statements should reference their own source file
    multiStmts.foreach(_.sourceFile should include("multi_statement.sql"))
    singleStmts.foreach(_.sourceFile should include("single_statement.sql"))
    commentStmts.foreach(_.sourceFile should include("with_comments.sql"))
  }
}
