package com.bob.sparkcoverage.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}

class DataSourceConfigSpec extends AnyFunSuite with Matchers {

  private def createTempDir(): Path = {
    val dir = Files.createTempDirectory("datasource_test_")
    dir.toFile.deleteOnExit()
    dir
  }

  private def createTempFile(dir: Path, name: String, content: String = ""): Path = {
    val file = dir.resolve(name)
    Files.write(file, content.getBytes("UTF-8"))
    file.toFile.deleteOnExit()
    file
  }

  test("resolve by convention when no config file") {
    val dataDir = createTempDir()
    createTempFile(dataDir, "orders.csv", "id,amount\n1,100")
    createTempFile(dataDir, "customers.csv", "id,name\n1,Alice")

    val result = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set("orders", "customers")
    )

    result should have size 2
    result("orders").getFileName.toString shouldBe "orders.csv"
    result("customers").getFileName.toString shouldBe "customers.csv"
    result("orders").getParent shouldBe dataDir
  }

  test("return empty map for empty required tables") {
    val dataDir = createTempDir()
    val result = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set.empty
    )
    result shouldBe empty
  }

  test("resolve from YAML config file") {
    val dataDir = createTempDir()
    val configDir = createTempDir()
    createTempFile(dataDir, "orders.csv")
    createTempFile(dataDir, "customers.csv")

    val configContent =
      s"""dataSources:
         |  orders: ${dataDir.resolve("orders.csv")}
         |  customers: ${dataDir.resolve("customers.csv")}
         |""".stripMargin

    val configFile = createTempFile(configDir, "config.yaml", configContent)

    val result = DataSourceConfig.resolve(
      configFile = Some(configFile),
      dataDir = dataDir,
      requiredTables = Set("orders", "customers")
    )

    result should have size 2
    Files.exists(result("orders")) shouldBe true
    Files.exists(result("customers")) shouldBe true
  }

  test("only resolve required tables") {
    val dataDir = createTempDir()
    createTempFile(dataDir, "orders.csv")
    createTempFile(dataDir, "customers.csv")

    val result = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set("orders")
    )

    result should have size 1
    result should contain key "orders"
    result should not contain key("customers")
  }

  test("handle missing CSV for required table") {
    val dataDir = createTempDir()
    createTempFile(dataDir, "orders.csv")

    val result = DataSourceConfig.resolve(
      configFile = None,
      dataDir = dataDir,
      requiredTables = Set("orders", "nonexistent")
    )

    result should contain key "orders"
    if (result.contains("nonexistent")) {
      Files.exists(result("nonexistent")) shouldBe false
    }
  }

  test("parse YAML config correctly") {
    val configDir = createTempDir()
    val configContent =
      """dataSources:
        |  orders: /data/orders.csv
        |  customers: /data/customers.csv
        |""".stripMargin
    val configFile = createTempFile(configDir, "config.yaml", configContent)

    val mappings = DataSourceConfig.parseYamlConfig(configFile)
    mappings should have size 2
    mappings("orders") shouldBe "/data/orders.csv"
    mappings("customers") shouldBe "/data/customers.csv"
  }

  test("parse YAML with comments and empty lines") {
    val configDir = createTempDir()
    val configContent =
      """# Config file
        |dataSources:
        |  # Order data
        |  orders: /data/orders.csv
        |
        |  customers: /data/customers.csv
        |""".stripMargin
    val configFile = createTempFile(configDir, "config.yaml", configContent)

    val mappings = DataSourceConfig.parseYamlConfig(configFile)
    mappings should have size 2
  }

  test("convention fallback for tables not in partial config") {
    val dataDir = createTempDir()
    val configDir = createTempDir()
    createTempFile(dataDir, "orders.csv")
    createTempFile(dataDir, "customers.csv")

    val configContent =
      s"""dataSources:
         |  orders: ${dataDir.resolve("orders.csv")}
         |""".stripMargin
    val configFile = createTempFile(configDir, "config.yaml", configContent)

    val result = DataSourceConfig.resolve(
      configFile = Some(configFile),
      dataDir = dataDir,
      requiredTables = Set("orders", "customers")
    )

    result should have size 2
    Files.exists(result("orders")) shouldBe true
    Files.exists(result("customers")) shouldBe true
  }
}
