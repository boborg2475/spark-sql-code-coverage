package com.bob.sparkcoverage.parser

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

import com.bob.sparkcoverage.model.CoverageModels.SqlStatement

class SqlFileParserSpec extends AnyFunSuite with Matchers {

  private def writeTempSql(content: String): Path = {
    val tmp = Files.createTempFile("test_", ".sql")
    Files.write(tmp, content.getBytes("UTF-8"))
    tmp.toFile.deleteOnExit()
    tmp
  }

  test("parse single statement with trailing semicolon") {
    val path = writeTempSql("SELECT * FROM users;")
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql shouldBe "SELECT * FROM users"
    stmts(0).statementIdx shouldBe 0
    stmts(0).lineNumber shouldBe 1
  }

  test("parse single statement without trailing semicolon") {
    val path = writeTempSql("SELECT * FROM users")
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql shouldBe "SELECT * FROM users"
  }

  test("parse multiple statements") {
    val sql = "SELECT 1;\nSELECT 2;\nSELECT 3;"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 3
    stmts(0).sql shouldBe "SELECT 1"
    stmts(1).sql shouldBe "SELECT 2"
    stmts(2).sql shouldBe "SELECT 3"
    stmts(0).statementIdx shouldBe 0
    stmts(1).statementIdx shouldBe 1
    stmts(2).statementIdx shouldBe 2
  }

  test("strip single-line comments") {
    val sql = "-- this is a comment\nSELECT * FROM users;"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql should not include "--"
    stmts(0).sql should include("SELECT")
  }

  test("strip block comments") {
    val sql = "/* block comment */\nSELECT * FROM users;"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql should not include "/*"
    stmts(0).sql should not include "*/"
    stmts(0).sql should include("SELECT")
  }

  test("strip multi-line block comments") {
    val sql = "/* multi\nline\ncomment */\nSELECT * FROM users;"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql should include("SELECT")
  }

  test("do not split on semicolons inside single-quoted strings") {
    val sql = "SELECT * FROM t WHERE x = 'a;b';"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql should include("'a;b'")
  }

  test("do not split on semicolons inside double-quoted strings") {
    val sql = """SELECT * FROM t WHERE x = "a;b";"""
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql should include("\"a;b\"")
  }

  test("handle empty file") {
    val path = writeTempSql("")
    val stmts = SqlFileParser.parse(path)
    stmts shouldBe empty
  }

  test("handle file with only whitespace") {
    val path = writeTempSql("   \n\n  \t  ")
    val stmts = SqlFileParser.parse(path)
    stmts shouldBe empty
  }

  test("handle file with only comments") {
    val path = writeTempSql("-- just a comment\n/* another comment */")
    val stmts = SqlFileParser.parse(path)
    stmts shouldBe empty
  }

  test("skip empty statements between semicolons") {
    val sql = "SELECT 1;;\n;\nSELECT 2;"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 2
    stmts(0).sql shouldBe "SELECT 1"
    stmts(1).sql shouldBe "SELECT 2"
  }

  test("preserve source file path") {
    val path = writeTempSql("SELECT 1;")
    val stmts = SqlFileParser.parse(path)
    stmts(0).sourceFile shouldBe path.toString
  }

  test("track line numbers for multi-line statements") {
    val sql = "SELECT\n  col1,\n  col2\nFROM t;\n\nSELECT col3 FROM t2;"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 2
    stmts(0).lineNumber shouldBe 1
    stmts(1).lineNumber should be > 1
  }

  test("do not strip comments inside string literals") {
    val sql = "SELECT * FROM t WHERE x = '-- not a comment';"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql should include("-- not a comment")
  }

  test("handle escaped single quotes") {
    val sql = "SELECT * FROM t WHERE x = 'it''s a test';"
    val path = writeTempSql(sql)
    val stmts = SqlFileParser.parse(path)
    stmts should have size 1
    stmts(0).sql should include("it''s a test")
  }
}
