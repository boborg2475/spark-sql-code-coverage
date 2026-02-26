package com.bob.sparkcoverage.model

import java.nio.file.Path

object CoverageModels {

  case class SqlStatement(
    sql: String,
    sourceFile: String,
    lineNumber: Int,
    statementIdx: Int
  )

  case class ColumnRef(
    table: Option[String],
    column: String
  )

  case class LineageEdge(
    output: ColumnRef,
    inputs: Set[ColumnRef],
    transformationSql: Option[String]
  )

  case class LineageGraph(
    edges: Seq[LineageEdge],
    expressionColumns: Map[String, Set[ColumnRef]]
  )

  case class CoverableExpression(
    id: String,
    expressionType: ExpressionType,
    context: String,
    sqlFragment: String,
    sourceFile: String,
    lineNumber: Int,
    lineageColumns: Set[ColumnRef]
  )

  case class ExpressionCoverage(
    expression: CoverableExpression,
    isCovered: Boolean,
    checkQuery: String
  )

  case class StatementCoverage(
    statement: SqlStatement,
    expressions: Seq[ExpressionCoverage],
    coveragePercent: Double
  )

  case class FileCoverage(
    filePath: String,
    statements: Seq[StatementCoverage],
    coveragePercent: Double
  )

  case class TypeCoverage(
    total: Int,
    covered: Int,
    coveragePercent: Double
  )

  case class CoverageResult(
    files: Seq[FileCoverage],
    byExpressionType: Map[ExpressionType, TypeCoverage],
    lineageGraph: LineageGraph,
    coveragePercent: Double
  )

  case class DataSource(
    tableName: String,
    filePath: Path,
    format: String = "csv"
  )
}
