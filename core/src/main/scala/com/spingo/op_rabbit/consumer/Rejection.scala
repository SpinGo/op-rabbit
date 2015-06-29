package com.spingo.op_rabbit.consumer

sealed trait Rejection extends Exception { val reason: String }

case class UnhandledExceptionRejection(reason: String, cause: Throwable = null) extends Exception(reason, cause) with Rejection
trait ExtractRejection extends Rejection

case class ParseExtractRejection(val reason: String, cause: Throwable = null) extends Exception(reason, cause) with ExtractRejection

object ExtractRejection {
  @deprecated("Use ParseExtractRejection")
  def apply(reason: String, cause: Throwable = null) = new ParseExtractRejection(reason, cause)
  def unapply(ex: ExtractRejection) =
    Some((ex.getMessage, ex.getCause))
}

case class ValueExpectedExtractRejection(reason: String, cause: Throwable = null) extends Exception(reason, cause) with ExtractRejection
case class NackRejection(reason: String) extends Exception(reason) with Rejection
