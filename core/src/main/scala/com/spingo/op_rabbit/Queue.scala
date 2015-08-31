package com.spingo.op_rabbit

import com.spingo.op_rabbit.properties.Header
import com.thenewmotion.akka.rabbitmq.Channel
import scala.concurrent.duration._

/**
  Binding which declare a message queue, without any exchange or topic bindings.

  @see [[Queue$.ModeledArgs$ Queue.ModeledArgs]], [[TopicBinding]], [[HeadersBinding]], [[FanoutBinding]], [[Subscription]]

  @param queueName    The name of the message queue to declare; the consumer paired with this binding will pull from this.
  @param durable      Specifies whether or not the message queue contents should survive a broker restart; default false.
  @param exclusive    Specifies whether or not other connections can see this connection; default false.
  @param autoDelete   Specifies whether this message queue should be deleted when the connection is closed; default false.
  @param arguments    Special arguments for this queue; See [[Queue$.ModeledArgs$ Queue.ModeledArgs]] for a list of valid arguments, and their effect.
  */
case class Queue(
  queueName: String,
  durable: Boolean = true,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  arguments: Seq[Header] = Seq()
) extends QueueDefinition {

  def declare(c: Channel): Unit = {
    c.queueDeclare(queueName, durable, exclusive, autoDelete,
      if (arguments.isEmpty)
        null
      else
        properties.toJavaMap(arguments)
    )
  }
}

object Queue {
  def passive(queueName: String): QueueDefinition = new QueueBindingPassive(queueName, None)
  def passive(binding: QueueDefinition): QueueDefinition = new QueueBindingPassive(binding.queueName, Some(binding))

  /**
    Collection of known queue arguments for RabbitMQ.
    */
  object ModeledArgs {
    import properties._

    /**
      Automatically drop any messages in the queue older than specified time.

      [[http://www.rabbitmq.com/ttl.html#per-queue-message-ttl Read more: TTL]]
      */
    val `x-message-ttl`: UnboundTypedHeader[FiniteDuration] = UnboundTypedHeaderLongToFiniteDuration("x-message-ttl")

    /**
      Delete the message queue after the provided duration of unuse; think RPC response queues which, due to error, may never be consumed.

      [[http://www.rabbitmq.com/ttl.html#queue-ttl Read more: TTL]]
      */
    val `x-expires`: UnboundTypedHeader[FiniteDuration] = UnboundTypedHeaderLongToFiniteDuration("x-expires")

    /**
      Declare a priority queue. Note: this value cannot be changed once a queue is already declared.

      [[http://www.rabbitmq.com/priority.html Read more: Priority Queue Support]]
      */
    val `x-max-priority` = TypedHeader[Byte]("x-max-priority")


    /**
      On a `dead letter` event (message is expired due to x-message-ttl,
      x-expires, or dropped due to x-max-length exceeded, etc.), route
      the message to the specified exchange.

      To specify a routing key, also, use [[x-dead-letter-routing-key]]

      [[http://www.rabbitmq.com/dlx.html Read more: Dead Letter Exchanges]]
      */
    val `x-dead-letter-exchange` = TypedHeader[String]("x-dead-letter-exchange")

    /**
      Specified which routing key should be used when routing a dead-letter to the dead-letter exchange.

      See [[x-dead-letter-exchange]]

      [[http://www.rabbitmq.com/dlx.html Read more: Dead Letter Exchanges]]
      */
    val `x-dead-letter-routing-key` = TypedHeader[String]("x-dead-letter-routing-key")

    /**
      Specify the maximum number of messages this queue should
      contain. Messages will be dropped or dead-lettered from the front
      of the queue to make room for new messages once the limit is
      reached.

      Must be a non-negative integer.

      [[http://www.rabbitmq.com/maxlength.html Read more: Queue Length Limit]]
      */
    val `x-max-length` = TypedHeader[Int]("x-max-length")

    /**
      Specify the maximum size, in bytes, that this queue should
      contain. Messages will be dropped or dead-lettered from the front
      of the queue to make room for new messages once the limit is
      reached.

      [[http://www.rabbitmq.com/maxlength.html Read more: Queue Length Limit]]
      */
    val `x-max-length-bytes` = TypedHeader[Int]("x-max-length-bytes")

  }
}
