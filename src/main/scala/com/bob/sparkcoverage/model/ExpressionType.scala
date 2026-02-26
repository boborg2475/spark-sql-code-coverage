package com.bob.sparkcoverage.model

sealed trait ExpressionType

object ExpressionType {
  case object CASE_BRANCH extends ExpressionType
  case object CASE_ELSE extends ExpressionType
  case object WHERE_PREDICATE extends ExpressionType
  case object JOIN_CONDITION extends ExpressionType
  case object HAVING_PREDICATE extends ExpressionType
  case object COALESCE_BRANCH extends ExpressionType
  case object IF_BRANCH extends ExpressionType
  case object NULLIF_BRANCH extends ExpressionType
  case object SUBQUERY_PREDICATE extends ExpressionType
  case object FILTER_PREDICATE extends ExpressionType
}
