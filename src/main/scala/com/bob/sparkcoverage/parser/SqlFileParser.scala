package com.bob.sparkcoverage.parser

import java.nio.file.{Files, Path}
import scala.collection.mutable

import com.bob.sparkcoverage.model.CoverageModels.SqlStatement

object SqlFileParser {

  def parse(path: Path): Seq[SqlStatement] = {
    val content = new String(Files.readAllBytes(path), "UTF-8")
    val cleaned = stripComments(content)
    splitStatements(cleaned, path.toString)
  }

  private def stripComments(sql: String): String = {
    val result = new StringBuilder
    var i = 0
    var inSingleQuote = false
    var inDoubleQuote = false

    while (i < sql.length) {
      if (inSingleQuote) {
        result.append(sql.charAt(i))
        if (sql.charAt(i) == '\'' && (i + 1 >= sql.length || sql.charAt(i + 1) != '\'')) {
          inSingleQuote = false
        } else if (sql.charAt(i) == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') {
          result.append(sql.charAt(i + 1))
          i += 1
        }
        i += 1
      } else if (inDoubleQuote) {
        result.append(sql.charAt(i))
        if (sql.charAt(i) == '"') {
          inDoubleQuote = false
        }
        i += 1
      } else if (sql.charAt(i) == '\'') {
        inSingleQuote = true
        result.append(sql.charAt(i))
        i += 1
      } else if (sql.charAt(i) == '"') {
        inDoubleQuote = true
        result.append(sql.charAt(i))
        i += 1
      } else if (i + 1 < sql.length && sql.charAt(i) == '-' && sql.charAt(i + 1) == '-') {
        // Line comment: skip to end of line, preserve the newline
        while (i < sql.length && sql.charAt(i) != '\n') {
          i += 1
        }
        // Don't consume the newline — it'll be appended on the next iteration
      } else if (i + 1 < sql.length && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
        // Block comment: skip to closing */, replace with equivalent whitespace to preserve line numbers
        i += 2
        while (i + 1 < sql.length && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
          if (sql.charAt(i) == '\n') {
            result.append('\n')
          }
          i += 1
        }
        if (i + 1 < sql.length) {
          i += 2 // skip */
        }
      } else {
        result.append(sql.charAt(i))
        i += 1
      }
    }

    result.toString()
  }

  private def splitStatements(sql: String, sourceFile: String): Seq[SqlStatement] = {
    val statements = mutable.Buffer[SqlStatement]()
    val current = new StringBuilder
    var i = 0
    var inSingleQuote = false
    var inDoubleQuote = false
    var stmtStartOffset = 0

    while (i < sql.length) {
      val ch = sql.charAt(i)

      if (inSingleQuote) {
        current.append(ch)
        if (ch == '\'' && (i + 1 >= sql.length || sql.charAt(i + 1) != '\'')) {
          inSingleQuote = false
        } else if (ch == '\'' && i + 1 < sql.length && sql.charAt(i + 1) == '\'') {
          current.append(sql.charAt(i + 1))
          i += 1
        }
        i += 1
      } else if (inDoubleQuote) {
        current.append(ch)
        if (ch == '"') {
          inDoubleQuote = false
        }
        i += 1
      } else if (ch == '\'') {
        inSingleQuote = true
        current.append(ch)
        i += 1
      } else if (ch == '"') {
        inDoubleQuote = true
        current.append(ch)
        i += 1
      } else if (ch == ';') {
        val trimmed = current.toString().trim
        if (trimmed.nonEmpty) {
          val lineNumber = countLineNumber(sql, stmtStartOffset)
          statements += SqlStatement(
            sql = trimmed,
            sourceFile = sourceFile,
            lineNumber = lineNumber,
            statementIdx = statements.size
          )
        }
        current.clear()
        i += 1
        stmtStartOffset = i
      } else {
        if (current.toString().trim.isEmpty && (ch == '\n' || ch == '\r' || ch == ' ' || ch == '\t')) {
          // Haven't started a real statement yet, advance the start offset
          if (ch == '\n' || (!current.toString().contains('\n') && current.toString().trim.isEmpty)) {
            // keep tracking
          }
        }
        current.append(ch)
        i += 1
      }
    }

    // Handle final statement without trailing semicolon
    val trimmed = current.toString().trim
    if (trimmed.nonEmpty) {
      val lineNumber = countLineNumber(sql, stmtStartOffset)
      statements += SqlStatement(
        sql = trimmed,
        sourceFile = sourceFile,
        lineNumber = lineNumber,
        statementIdx = statements.size
      )
    }

    statements.toSeq
  }

  private def countLineNumber(text: String, offset: Int): Int = {
    // Find the start of actual content (skip leading whitespace)
    val searchEnd = math.min(offset, text.length)
    val baseLineNumber = text.substring(0, searchEnd).count(_ == '\n') + 1

    // Advance past any leading whitespace in the current segment to find where content starts
    var contentStart = offset
    while (contentStart < text.length && (text.charAt(contentStart) == ' ' || text.charAt(contentStart) == '\t' || text.charAt(contentStart) == '\n' || text.charAt(contentStart) == '\r')) {
      contentStart += 1
    }
    val extraNewlines = text.substring(searchEnd, math.min(contentStart, text.length)).count(_ == '\n')

    baseLineNumber + extraNewlines
  }
}
