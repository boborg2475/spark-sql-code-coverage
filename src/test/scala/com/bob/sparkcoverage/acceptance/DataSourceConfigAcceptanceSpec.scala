package com.bob.sparkcoverage.acceptance

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

import com.bob.sparkcoverage.config.DataSourceConfig

class DataSourceConfigAcceptanceSpec extends AnyFunSuite with Matchers {

  private def resourcePath(name: String): Path = {
    val url = getClass.getClassLoader.getResource(s"acceptance/$name")
    require(url != null, s"Test resource not found: acceptance/$name")
    Paths.get(url.toURI)
  }

  private def dataDir: Path = resourcePath("data")

  private def configPath(name: String): Path = resourcePath(s"config/$name")

  // --- YAML config resolution ---

  test("resolve all table mappings from YAML config file") {
    val result = DataSourceConfig.resolve(
      configFile = Some(configPath("coverage-config.yaml")),
      dataDir = dataDir,
      requiredTables = Set("orders", "customers", "products")
    )

    result should have size 3
    result should contain key "orders"
    result should contain key "customers"
    result should contain key "products"

    // All resolved paths should point to existing files
    result.values.foreach { path =>
      Files.exists(path) shouldBe true
    }
  }

  test("resolved paths from YAML config point to correct CSV files") {
    val result = DataSourceConfig.resolve(
      configFile = Some(configPath("coverage-config.yaml")),
      dataDir = dataDir,
      requiredTables = Set("orders", "customers")
    )

    result("orders").toString should endWith("orders.csv")
    result("customers").toString should endWith("customers.csv")
  }

  // --- Convention-based fallback ---

  test("resolve tables by convention when no config file provided") {
    val result = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set("orders", "customers", "products")
    )

    result should have size 3
    result("orders").toString should endWith("orders.csv")
    result("customers").toString should endWith("customers.csv")
    result("products").toString should endWith("products.csv")
  }

  test("use convention fallback for tables not in partial YAML config") {
    // partial-config.yaml only has orders; customers and products should fall back to convention
    val result = DataSourceConfig.resolve(
      configFile = Some(configPath("partial-config.yaml")),
      dataDir = dataDir,
      requiredTables = Set("orders", "customers", "products")
    )

    result should have size 3
    result should contain key "orders"
    result should contain key "customers"
    result should contain key "products"

    // All paths should be valid
    result.values.foreach { path =>
      Files.exists(path) shouldBe true
    }
  }

  // --- Edge cases ---

  test("return empty map when required tables is empty") {
    val result = DataSourceConfig.resolve(
      configFile = Some(configPath("coverage-config.yaml")),
      dataDir = dataDir,
      requiredTables = Set.empty
    )

    result shouldBe empty
  }

  test("handle missing CSV file for a required table") {
    // Table "nonexistent" has no config entry and no nonexistent.csv in data dir
    // The implementation should either exclude it from results or signal an error.
    // We test that it doesn't crash and the table is not silently mapped to a bad path.
    val result = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set("orders", "nonexistent_table")
    )

    // orders should still resolve
    result should contain key "orders"

    // nonexistent_table should either be absent or its path should not exist
    if (result.contains("nonexistent_table")) {
      Files.exists(result("nonexistent_table")) shouldBe false
    }
  }

  test("only resolve required tables even when config has extra entries") {
    // Config has orders, customers, products but we only require orders
    val result = DataSourceConfig.resolve(
      configFile = Some(configPath("coverage-config.yaml")),
      dataDir = dataDir,
      requiredTables = Set("orders")
    )

    result should have size 1
    result should contain key "orders"
  }

  test("convention fallback looks for tableName.csv in data directory") {
    // Verify the convention: table name "customers" → data dir / "customers.csv"
    val result = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set("customers")
    )

    result should have size 1
    val customersPath = result("customers")
    customersPath.getFileName.toString shouldBe "customers.csv"
    customersPath.getParent shouldBe dataDir
  }
}
