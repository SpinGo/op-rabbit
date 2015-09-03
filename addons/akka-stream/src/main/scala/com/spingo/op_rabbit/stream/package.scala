package com.spingo.op_rabbit

/**
  == Batteries not included ==

  To use this package, you must add `'op-rabbit-akka-stream'` to your dependencies.

  == Overview ==

  @see [[https://github.com/SpinGo/op-rabbit/tree/master/addons/akka-stream/src/test/scala/com/spingo/op_rabbit Akka Stream Specs on GitHub]]
  */
package object stream {

  import scala.concurrent.{Promise, ExecutionContext}

  /**
    Used by [[MessagePublisherSink]] to fail elements in the case the [[https://www.rabbitmq.com/confirms.html RabbitMQ broker "negatively acknowledges" a published message]].
    */
  class MessageNacked(id: Long) extends Exception(s"Published message with id ${id} was nacked by the broker.")
}
