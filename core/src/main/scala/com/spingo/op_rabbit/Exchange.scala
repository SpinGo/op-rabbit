package com.spingo.op_rabbit

import com.spingo.op_rabbit.properties.Header
import com.newmotion.akka.rabbitmq.Channel
import com.spingo.op_rabbit.Binding._

/**
  Common interface for how exchanges are defined.

  See:

  - [[Exchange$ Exchange object]] for various factory definitions.
  - [[Exchange$.ModeledArgs$ Exchange.ModeledArgs]]
  */
trait Exchange[+T <: Exchange.Value] extends ExchangeDefinition[Concrete] {
}

private class ExchangeImpl[+T <: Exchange.Value](
  val exchangeName: String,
  kind: T,
  durable: Boolean,
  autoDelete: Boolean,
  arguments: Seq[Header]) extends Exchange[T] {
  def declare(c: Channel): Unit =
    c.exchangeDeclare(exchangeName, kind.toString, durable, autoDelete, properties.toJavaMap(arguments))
}

/**
  Passively declare an exchange. If the queue does not exist already,
  then either try the fallback binding, or fail.

  See RabbitMQ Java client docs, [[https://www.rabbitmq.com/releases/rabbitmq-java-client/v3.5.4/rabbitmq-java-client-javadoc-3.5.4/com/rabbitmq/client/Channel.html#exchangeDeclarePassive(java.lang.String) Channel.exchangeDeclarePassive]].
  */
private class ExchangePassive[T <: Exchange.Value](val exchangeName: String, ifNotDefined: Option[Exchange[T]] = None)
    extends Exchange[T] {
  def declare(channel: Channel): Unit = {
    RabbitHelpers.tempChannel(channel.getConnection) { t =>
      t.exchangeDeclarePassive(exchangeName)
    }.left.foreach { (ex =>
      ifNotDefined.map(_.declare(channel)) getOrElse { throw ex })
    }
  }
}

object Exchange extends Enumeration {
  trait Abstract {
    val exchangeName: String
    def declare(channel: Channel): Unit
  }
  val Topic = Value("topic")
  val Headers = Value("headers")
  val Fanout = Value("fanout")
  val Direct = Value("direct")

  def topic(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()):
      Exchange[Exchange.Topic.type] = new ExchangeImpl(name: String, Exchange.Topic, durable, autoDelete, arguments)
  def headers(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()):
      Exchange[Exchange.Headers.type] = new ExchangeImpl(name: String, Exchange.Headers, durable, autoDelete, arguments)
  def fanout(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()):
      Exchange[Exchange.Fanout.type] = new ExchangeImpl(name: String, Exchange.Fanout, durable, autoDelete, arguments)
  def direct(name: String, durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()):
      Exchange[Exchange.Direct.type] = new ExchangeImpl(name: String, Exchange.Direct, durable, autoDelete, arguments)

  val default: Exchange[Exchange.Direct.type] = new Exchange[Exchange.Direct.type] {
    val exchangeName = ""
    def declare(c: Channel): Unit = {
      // no-op
    }
  }

  def passive(exchangeName: String): Exchange[Nothing] = new ExchangePassive(exchangeName, None)
  def passive[T <: Exchange.Value](binding: Exchange[T]): Exchange[T] = new ExchangePassive(binding.exchangeName, Some(binding))

  /**
    Collection of known exchange arguments for RabbitMQ.
    */
  object ModeledArgs {
    import properties._

    /**
      Specify that RabbitMQ should forward to the specified alternate-exchange in the event that it is unable to route the message to any queue on this exchange.

      [[http://www.rabbitmq.com/ae.html Read more]]
      */
    val `alternate-exchange` = TypedHeader[String]("alternate-exchange")
  }
}
