package com.spingo.op_rabbit

sealed trait Rejection extends Exception { val reason: String }

object Rejection {
  sealed trait ExtractRejection extends Rejection

  case class ParseExtractRejection(val reason: String, cause: Throwable = null) extends Exception(reason, cause) with ExtractRejection

  case class ValueExpectedExtractRejection(reason: String, cause: Throwable = null) extends Exception(reason, cause) with ExtractRejection
}
