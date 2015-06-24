package com.spingo.op_rabbit

import akka.actor.ActorSystem
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.Channel
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Try
import com.spingo.op_rabbit.properties._
/**
  Basic interface; send to [[RabbitControl]] actor for delivery.
  */
trait MessageForPublicationLike extends (Channel => Unit) {
  val dropIfNoChannel: Boolean
}

object MessageForPublicationLike {
  type Factory[T, M <: MessageForPublicationLike] = (T => M)
  val defaultProperties = List(properties.DeliveryMode.persistent)
}

/**
  Common interface for publication strategies

  @see [[TopicPublisher]], [[QueuePublisher]]
  */
trait MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit
}

/**
  Publishes messages to specified topic; note that this is a strategy which receives message data and publishes it to a channel.

  @param routingKey The routing key (or topic)
  @param exchange The exchange to which the strategy will publish the message

  @see [[QueuePublisher]], [[MessageForPublication]]
  */
case class TopicPublisher(routingKey: String, exchange: String = RabbitControl topicExchangeName) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish(exchange, routingKey, properties, data)
}

/**
  Publishes messages directly to the specified message-queue; note that this is a strategy which receives message data and publishes it to a channel.

  @see [[TopicPublisher]], [[MessageForPublication]]
  */
case class QueuePublisher(queue: String) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish("", queue, properties, data)
}

/**
  Describes a messages data, along with publication strategy; send to [[RabbitControl]] actor for delivery.

  Use the factory method [[MessageForPublication$.apply]] to instantiate one of these using a [[RabbitMarshaller]].

  @see [[MessageForPublication$]]
  */
class UnconfirmedMessage(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties,
  val dropIfNoChannel: Boolean = false) extends MessageForPublicationLike {
  def apply(c: Channel) = publisher(c, data, properties)
}

object UnconfirmedMessage {

  /**
    Factory method for instantiating a MessageForPublication

    Note, serialization occurs in the thread calling the constructor, not the actor thread responsible for sending messages.

    @param publisher [[MessagePublisher]] which defines how and to where this message will be published.
    @param message The message data.
    @param dropIfNoChannel If a channel is not available at the time this message is sent, queue the message up internally and send as soon as a connection is available. Default true.
    @param marshaller The implicit [[RabbitMarshaller]] used to serialize data T to binary.
    */
  def apply[T](publisher: MessagePublisher, message: T, properties: Seq[MessageProperty] = Seq.empty, dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]) = {
    factory[T](publisher, properties, dropIfNoChannel)(marshaller)(message)
  }

  def factory[T](publisher: MessagePublisher, properties: Seq[MessageProperty] = Seq.empty, dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]): MessageForPublicationLike.Factory[T, UnconfirmedMessage] = {
    val builder = builderWithProperties(MessageForPublicationLike.defaultProperties ++ properties)
    marshaller.properties(builder)
    val rabbitProperties = builder.build

    { (message) => new UnconfirmedMessage(publisher, marshaller.marshall(message), rabbitProperties, dropIfNoChannel) }
  }
}

final class ConfirmedMessage(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties) extends MessageForPublicationLike {
  val dropIfNoChannel: Boolean = false
  protected [op_rabbit] val publishedPromise = Promise[Unit]
  val published = publishedPromise.future
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

object TopicMessage {
  def apply[T](message: T, routingKey: String, exchange: String = RabbitControl.topicExchangeName, properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): ConfirmedMessage =
    ConfirmedMessage(TopicPublisher(routingKey, exchange), message, properties)
}

object QueueMessage {
  def apply[T](
    message: T,
    queue: String,
    properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): ConfirmedMessage =
    ConfirmedMessage(QueuePublisher(queue), message, properties)
}
