package com.spingo.op_rabbit

/**
  == Batteries not included ==

  To use this package, you must add `'op-rabbit-akka-stream'` to your dependencies.

  == Overview ==

  @see [[https://github.com/SpinGo/op-rabbit/tree/master/addons/akka-stream/src/test/scala/com/spingo/op_rabbit Akka Stream Specs on GitHub]]
  */
package object stream {

  import scala.concurrent.{Promise, ExecutionContext}

  type AckTup[T] = (Promise[Unit], T)
  case object MessageNacked extends Exception(s"A published message was nacked by the broker.")

  @deprecated("ConfirmedPublisherSink has been renamed to MessagePublisherSink", "v1.0.0-RC3")
  val ConfirmedPublisherSink = MessagePublisherSink
}
