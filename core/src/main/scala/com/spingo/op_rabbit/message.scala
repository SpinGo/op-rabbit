package com.spingo.op_rabbit

import akka.actor.ActorSystem
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Channel
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Try
import com.spingo.op_rabbit.properties._

object ModeledMessageHeaders {
  import properties._

  /**
    This message header causes RabbitMQ to drop a message once it's reached the head of a queue, if it's older than the provided duration.

    [[http://www.rabbitmq.com/ttl.html#per-message-ttl Read more]]
    */
  val `x-message-ttl`: UnboundTypedHeader[FiniteDuration] = UnboundTypedHeaderLongToFiniteDuration("x-expires")
}

/**
  Basic interface; send to [[RabbitControl]] actor for delivery.
  */
trait MessageForPublicationLike extends (Channel => Unit) {
  val dropIfNoChannel: Boolean
}

object MessageForPublicationLike {
  type Factory[T, M <: MessageForPublicationLike] = (T => M)
  val defaultProperties = List(properties.DeliveryModePersistence(true))
}

/**
  Common interface for publication strategies

  @see [[TopicPublisher]], [[QueuePublisher]]
  */
trait MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit
}

/**
  Publishes messages to specified exchange, with topic specified

  @param routingKey The routing key (or topic)
  @param exchange The exchange to which the strategy will publish the message

  @see [[QueuePublisher]], [[MessageForPublicationLike]]
  */
case class TopicPublisher(routingKey: String, exchange: String = RabbitControl topicExchangeName) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish(exchange, routingKey, properties, data)
}

/**
  Publishes messages to specified exchange

  @param routingKey The routing key (or topic)
  @param exchange The exchange to which the strategy will publish the message

  @see [[QueuePublisher]], [[MessageForPublicationLike]]
  */
case class ExchangePublisher(exchange: String) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish(exchange, "", properties, data)
}

/**
  Publishes messages directly to the specified message-queue

  @see [[TopicPublisher]], [[MessageForPublicationLike]]
  */
case class QueuePublisher(queue: String) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish("", queue, properties, data)
}

/**
  Publishes messages directly to the specified message-queue; on first message, verifies that the destination queue exists, returning an exception if not.

  This is useful if you want to prevent publishing to a non-existent queue
  */
case class VerifiedQueuePublisher(queue: String) extends MessagePublisher {
  private var verified = false
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit = {
    if (!verified) {
      RabbitHelpers.tempChannel(c.getConnection) { _.queueDeclarePassive(queue) } match {
        case Left(ex) => throw ex
        case _ => ()
      }
      verified = true
    }
    c.basicPublish("", queue, properties, data)
  }
}

/**
  Contains the message's data, along with publication strategy; send to [[RabbitControl]] actor for delivery. Upon delivery confirmation, [[RabbitControl]] will respond to the sender with `true`.

  Use the factory method [[ConfirmedMessage$.apply]] to instantiate one of these using an implicit [[RabbitMarshaller]] for serialization.

  @see [[ConfirmedMessage$]], [[TopicMessage$]], [[QueueMessage$]]
  */
final class ConfirmedMessage(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties) extends MessageForPublicationLike {
  val dropIfNoChannel = false
  def apply(c: Channel) = publisher(c, data, properties)
}

object ConfirmedMessage {
  def apply[T](publisher: MessagePublisher, message: T, properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]) = {
    factory[T](publisher, properties)(marshaller)(message)
  }

  def factory[T](publisher: MessagePublisher, properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): MessageForPublicationLike.Factory[T, ConfirmedMessage] = {
    val builder = builderWithProperties(MessageForPublicationLike.defaultProperties ++ properties)
    marshaller.properties(builder)
    val rabbitProperties = builder.build

    { (message) => new ConfirmedMessage(publisher, marshaller.marshall(message), rabbitProperties) }
  }

}

final class UnconfirmedMessage(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties) extends MessageForPublicationLike {
  val dropIfNoChannel = true
  def apply(c: Channel) = publisher(c, data, properties)
}

object UnconfirmedMessage {
  def apply[T](publisher: MessagePublisher, message: T, properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]) = {
    factory[T](publisher, properties)(marshaller)(message)
  }

  def factory[T](publisher: MessagePublisher, properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): MessageForPublicationLike.Factory[T, UnconfirmedMessage] = {
    val builder = builderWithProperties(MessageForPublicationLike.defaultProperties ++ properties)
    marshaller.properties(builder)
    val rabbitProperties = builder.build

    { (message) => new UnconfirmedMessage(publisher, marshaller.marshall(message), rabbitProperties) }
  }

}

object StatusCheckMessage {
  case class CheckException(msg: String) extends Exception(msg)
}
/**
  Send this message to RabbitControl to check the status of our connection to the RabbitMQ broker.
  */
class StatusCheckMessage(timeout: Duration = 5 seconds)(implicit actorSystem: ActorSystem) extends MessageForPublicationLike {
  val dropIfNoChannel = true
  private val isOpenPromise = Promise[Unit]

  /**
    Future fails with [[StatusCheckMessage$.CheckException CheckException]] if connection is not okay
    */
  val okay = isOpenPromise.future

  private def withTimeout[T](what:String, duration: FiniteDuration)(f: => Future[T]): Future[T] = {
    import actorSystem.dispatcher
    val timer = akka.pattern.after(duration, using = actorSystem.scheduler) {
      Future.failed(new scala.concurrent.TimeoutException(s"Response not received from ${what} after ${duration}."))
    }
    Future.firstCompletedOf(timer :: f :: Nil)
  }

  def apply(c: com.rabbitmq.client.Channel): Unit = {
    isOpenPromise.tryComplete(Try {
      assert(c.isOpen(), new StatusCheckMessage.CheckException("RabbitMQ outbound channel is not open"))
    })
  }
}

/**
  Shorthand for [[ConfirmedMessage$.apply ConfirmedMessage]](TopicPublisher(...), ...)
  */
object TopicMessage {
  def apply[T](message: T, routingKey: String, exchange: String = RabbitControl.topicExchangeName, properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): ConfirmedMessage =
    ConfirmedMessage(TopicPublisher(routingKey, exchange), message, properties)
}

/**
  Shorthand for [[ConfirmedMessage$.apply ConfirmedMessage]](QueuePublisher(...), ...)
  */
object QueueMessage {
  def apply[T](
    message: T,
    queue: String,
    properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): ConfirmedMessage =
    ConfirmedMessage(QueuePublisher(queue), message, properties)
}
