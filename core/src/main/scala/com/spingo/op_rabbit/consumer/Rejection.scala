package com.spingo.op_rabbit.consumer

sealed trait Rejection extends Exception { val reason: String }

case class UnhandledExceptionRejection(reason: String, cause: Throwable = null) extends Exception(reason, cause) with Rejection
case class ExtractRejection(reason: String) extends Exception(reason) with Rejection
case class NackRejection(reason: String) extends Exception(reason) with Rejection
