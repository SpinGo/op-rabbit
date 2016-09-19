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
  '''Confirmed''' Message container. Send to [[RabbitControl]] actor for delivery. Upon delivery confirmation, [[RabbitControl]] will respond to the sender with `true`.

  Use the factory method [[Message$.apply]] to instantiate one of these using an implicit [[RabbitMarshaller]] for serialization.

  @see [[Message$.exchange]], [[Message$.topic]], [[Message$.queue]]
  */
final class Message(
  val publisher: Publisher,
  val data: Array[Byte],
  val properties: BasicProperties) extends MessageForPublicationLike {
  val dropIfNoChannel = false
  val id: Long = Message.nextMessageId
  def apply(c: Channel) = publisher(c, data, properties)
}

private [op_rabbit] trait MessageFactory[M <: MessageForPublicationLike] {
  @inline
  def newInstance(publisher: Publisher, body: Array[Byte], properties: BasicProperties): M

  def apply[T](body: T, publisher: Publisher, properties: Seq[MessageProperty] = Seq())(
    implicit marshaller: RabbitMarshaller[T]) = {
    val builder = builderWithProperties(MessageForPublicationLike.defaultProperties ++ properties)
    marshaller.setProperties(builder)
    newInstance(publisher, marshaller.marshall(body), builder.build)
  }

  def factory[T](publisher: Publisher, properties: Seq[MessageProperty] = Seq.empty)(
    implicit marshaller: RabbitMarshaller[T]): MessageForPublicationLike.Factory[T, M] = {
    val builder = builderWithProperties(MessageForPublicationLike.defaultProperties ++ properties)
    marshaller.setProperties(builder)
    val rabbitProperties = builder.build

    { (body) => newInstance(publisher, marshaller.marshall(body), rabbitProperties) }
  }

  /**
    Shorthand for [[.apply Message]](Publisher.exchange(...), ...)
    */
  def exchange[T](message: T, exchange: String, routingKey: String = "", properties: Seq[MessageProperty] = Seq.empty)(
    implicit marshaller: RabbitMarshaller[T]): M =
    apply(message, Publisher.exchange(exchange, routingKey), properties)

  /**
    Shorthand for [[.apply Message]](Publisher.topic(...), ...)
    */
  def topic[T](
    message: T,
    routingKey: String,
    exchange: String = RabbitControl.topicExchangeName,
    properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): M =
    apply(message, Publisher.exchange(exchange, routingKey), properties)

  /**
    Shorthand for [[.apply Message]](Publisher.queue(...), ...)
    */
  def queue[T](
    message: T,
    queue: String,
    properties: Seq[MessageProperty] = Seq.empty)(implicit marshaller: RabbitMarshaller[T]): M  =
    apply(message, Publisher.queue(queue), properties)
}

/**
  '''Confirmed''' message generator. See [[Message$]]
  */
object Message extends MessageFactory[Message] {
  @inline
  def newInstance(publisher: Publisher, body: Array[Byte], properties: BasicProperties): Message =
    new Message(publisher, body, properties)

  private val messageSequence = new java.util.concurrent.atomic.AtomicLong()
  def nextMessageId = messageSequence.getAndIncrement

  sealed trait ConfirmResponse { val id: Long }
  case class Ack(id: Long) extends ConfirmResponse
  case class Nack(id: Long) extends ConfirmResponse
  case class Fail(id: Long, exception: Throwable) extends ConfirmResponse
}

final class UnconfirmedMessage(
  val publisher: Publisher,
  val data: Array[Byte],
  val properties: BasicProperties) extends MessageForPublicationLike {
  val dropIfNoChannel = true
  def apply(c: Channel) = publisher(c, data, properties)
}

object UnconfirmedMessage extends MessageFactory[UnconfirmedMessage] {
  @inline
  def newInstance(publisher: Publisher, body: Array[Byte], properties: BasicProperties) =
    new UnconfirmedMessage(publisher, body, properties)
}

object StatusCheckMessage {
  case class CheckException(msg: String) extends Exception(msg)
}
/**
  Send this message to RabbitControl to check the status of our connection to the RabbitMQ broker.
  */
class StatusCheckMessage(timeout: Duration = 5 seconds)(implicit actorSystem: ActorSystem)
    extends MessageForPublicationLike {
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
