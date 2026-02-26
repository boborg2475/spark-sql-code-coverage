package com.bob.sparkcoverage.acceptance

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

import com.bob.sparkcoverage.parser.SqlFileParser
import com.bob.sparkcoverage.config.DataSourceConfig

class Phase1IntegrationAcceptanceSpec extends AnyFunSuite with Matchers {

  private def resourcePath(name: String): Path = {
    val url = getClass.getClassLoader.getResource(s"acceptance/$name")
    require(url != null, s"Test resource not found: acceptance/$name")
    Paths.get(url.toURI)
  }

  private def sqlPath(name: String): Path = resourcePath(s"sql/$name")
  private def dataDir: Path = resourcePath("data")
  private def configPath(name: String): Path = resourcePath(s"config/$name")

  // --- End-to-end: Parse SQL + resolve data sources ---

  test("parse SQL file and resolve referenced tables via YAML config") {
    // Step 1: Parse a SQL file
    val statements = SqlFileParser.parse(sqlPath("multi_statement.sql"))
    statements should not be empty

    // Step 2: These SQL statements reference tables: orders, customers, products
    val requiredTables = Set("orders", "customers", "products")

    // Step 3: Resolve data sources using YAML config
    val dataSources = DataSourceConfig.resolve(
      configFile = Some(configPath("coverage-config.yaml")),
      dataDir = dataDir,
      requiredTables = requiredTables
    )

    // All required tables should be resolved
    dataSources should have size 3
    requiredTables.foreach { table =>
      dataSources should contain key table
      Files.exists(dataSources(table)) shouldBe true
    }
  }

  test("parse multiple SQL files and resolve all referenced tables from shared config") {
    // Parse multiple files
    val multiStmts = SqlFileParser.parse(sqlPath("multi_statement.sql"))
    val complexStmts = SqlFileParser.parse(sqlPath("complex_query.sql"))

    val allStatements = multiStmts ++ complexStmts
    allStatements.size should be >= 4

    // Both files reference orders, customers, and products
    val requiredTables = Set("orders", "customers", "products")

    val dataSources = DataSourceConfig.resolve(
      configFile = Some(configPath("coverage-config.yaml")),
      dataDir = dataDir,
      requiredTables = requiredTables
    )

    dataSources should have size 3
    dataSources.values.foreach { path =>
      Files.exists(path) shouldBe true
    }
  }

  test("parse SQL and resolve tables using convention fallback (no config file)") {
    val statements = SqlFileParser.parse(sqlPath("single_statement.sql"))
    statements should have size 1

    // single_statement.sql references the "customers" table
    val dataSources = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set("customers")
    )

    dataSources should have size 1
    dataSources should contain key "customers"
    Files.exists(dataSources("customers")) shouldBe true
  }

  test("end-to-end realistic scenario: multi-join SQL with mixed config resolution") {
    // Parse complex SQL with JOINs across multiple tables
    val statements = SqlFileParser.parse(sqlPath("complex_query.sql"))
    statements should have size 2

    // First statement JOINs orders, customers, products
    val firstSql = statements(0).sql.toLowerCase
    firstSql should include("join")

    // Resolve with partial config (orders explicit, rest via convention)
    val dataSources = DataSourceConfig.resolve(
      configFile = Some(configPath("partial-config.yaml")),
      dataDir = dataDir,
      requiredTables = Set("orders", "customers", "products")
    )

    dataSources should have size 3

    // Verify each CSV has actual data (non-empty files)
    dataSources.values.foreach { path =>
      Files.exists(path) shouldBe true
      Files.size(path) should be > 0L
    }

    // Verify statement metadata is correct
    statements(0).statementIdx shouldBe 0
    statements(1).statementIdx shouldBe 1
    statements(0).sourceFile should include("complex_query.sql")
    statements(1).sourceFile should include("complex_query.sql")
  }
}
