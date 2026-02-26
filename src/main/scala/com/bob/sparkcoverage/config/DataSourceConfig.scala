package com.bob.sparkcoverage.config

import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable
import scala.io.Source

object DataSourceConfig {

  def resolve(
    configFile: Option[Path],
    dataDir: Path,
    requiredTables: Set[String]
  ): Map[String, Path] = {
    val yamlMappings = configFile.map(parseYamlConfig).getOrElse(Map.empty)

    requiredTables.iterator.flatMap { tableName =>
      val resolved = yamlMappings.get(tableName) match {
        case Some(yamlPath) => resolveYamlPath(yamlPath, configFile.get, dataDir)
        case None => resolveByConvention(tableName, dataDir)
      }
      resolved.map(tableName -> _)
    }.toMap
  }

  private def resolveYamlPath(yamlValue: String, configFile: Path, dataDir: Path): Option[Path] = {
    val yamlPath = Paths.get(yamlValue)

    // If absolute path, use directly
    if (yamlPath.isAbsolute) {
      return Some(yamlPath)
    }

    // Try resolving relative to config file's parent directory
    val relativeToConfig = configFile.getParent.resolve(yamlPath).normalize()
    if (Files.exists(relativeToConfig)) {
      return Some(relativeToConfig)
    }

    // Fall back to convention: use just the filename in the data directory
    val fileName = yamlPath.getFileName
    Some(dataDir.resolve(fileName))
  }

  private def resolveByConvention(tableName: String, dataDir: Path): Option[Path] = {
    val path = dataDir.resolve(tableName + ".csv")
    if (Files.exists(path)) {
      Some(path)
    } else {
      Some(path) // Return the path even if it doesn't exist; caller can check
    }
  }

  private[config] def parseYamlConfig(configFile: Path): Map[String, String] = {
    val lines = Source.fromFile(configFile.toFile, "UTF-8").getLines().toSeq
    val mappings = mutable.Map[String, String]()

    var inDataSources = false
    for (line <- lines) {
      val trimmed = line.trim
      if (trimmed == "dataSources:") {
        inDataSources = true
      } else if (inDataSources && trimmed.nonEmpty && !trimmed.startsWith("#")) {
        if (!line.startsWith(" ") && !line.startsWith("\t")) {
          // New top-level key, stop parsing dataSources
          inDataSources = false
        } else {
          // Parse "  tableName: path" entries
          val colonIdx = trimmed.indexOf(':')
          if (colonIdx > 0) {
            val key = trimmed.substring(0, colonIdx).trim
            val value = trimmed.substring(colonIdx + 1).trim
            if (key.nonEmpty && value.nonEmpty) {
              mappings(key) = value
            }
          }
        }
      }
    }

    mappings.toMap
  }
}
