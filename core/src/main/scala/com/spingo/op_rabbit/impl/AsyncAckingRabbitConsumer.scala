package com.spingo.op_rabbit.impl

import akka.actor.{Actor, ActorLogging, ActorSystem, Terminated}
import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit._
import com.thenewmotion.akka.rabbitmq.{Channel, DefaultConsumer, Envelope}
import com.spingo.op_rabbit.subscription.{Rejection, ExtractRejection, UnhandledExceptionRejection, NackRejection}
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import com.spingo.op_rabbit.subscription.{Handler,Result}

// TODO - implement retry according to this pattern: http://yuserinterface.com/dev/2013/01/08/how-to-schedule-delay-messages-with-rabbitmq-using-a-dead-letter-exchange/
// - create two direct exchanges: work and retry
// - Publish to rety with additional header set/incremented: x-redelivers
// -  { "x-dead-letter-exchange", WORK_EXCHANGE },
// -  { "x-message-ttl", RETRY_DELAY }
// -   (setting since nothing consumes the direct exchange, it will go back to retry queue)
protected [op_rabbit] class AsyncAckingRabbitConsumer[T](
  name: String,
  queueName: String,
  recoveryStrategy: com.spingo.op_rabbit.subscription.RecoveryStrategy,
  rabbitErrorLogging: RabbitErrorLogging,
  handle: Handler)(implicit executionContext: ExecutionContext) extends Actor with ActorLogging {

  import Consumer._

  var pendingDeliveries = scala.collection.mutable.Set.empty[Long]

  case class RejectOrAck(ack: Boolean, deliveryTag: Long)

  context watch self

  def receive = {
    case Subscribe(channel) =>
      val consumerTag = setupSubscription(channel)
      context.become(connected(channel, Some(consumerTag)))
    case Unsubscribe =>
      sender ! true
      ()
    case Abort =>
      context stop self
      context.become(stopped)
    case Shutdown =>
      context stop self
      context.become(stopped)
    case Terminated(ref) if ref == self =>
      ()
  }

  def connected(channel: Channel, consumerTag: Option[String]): Receive = {
    case Subscribe(newChannel) =>
      if (channel != newChannel)
        pendingDeliveries.clear()

      val newConsumerTag = setupSubscription(newChannel)
      context.become(connected(newChannel, Some(newConsumerTag)))
    case Unsubscribe =>
      handleUnsubscribe(channel, consumerTag)
      sender ! true
      context.become(connected(channel, None))
    case delivery: Delivery =>
      handleDelivery(channel, delivery)
    case RejectOrAck(ack, consumerTag) =>
      handleRejectOrAck(ack, channel, consumerTag)
    case Shutdown =>
      handleUnsubscribe(channel, consumerTag)
      if(pendingDeliveries.isEmpty) {
        context stop self
        context.become(stopped)
      } else {
        context.become(stopping(channel))
      }
    case Abort =>
      context stop self
      context.become(stopped)
    case Terminated(ref) if ref == self =>
      handleUnsubscribe(channel, consumerTag)
      context.become(stopping(channel))
  }

  def stopping(channel: Channel): Receive = {
    case Subscribe(newChannel) =>
      // we lost our connection while stopping? Just bail. Nothing more to do.
      if (newChannel != channel) {
        pendingDeliveries.clear
        context stop self
      }
    case Unsubscribe =>
      sender ! true
    case RejectOrAck(ack, consumerTag) =>
      handleRejectOrAck(ack, channel, consumerTag)
      if (pendingDeliveries.isEmpty)
        context stop self
    case Delivery(consumerTag, envelope, properties, body) =>
      // note! Before RabbitMQ 2.7.0 does not preserve message order when this happens!
      // https://www.rabbitmq.com/semantics.html
      channel.basicReject(envelope.getDeliveryTag, true)
    case Shutdown =>
      ()
    case Abort =>
      context stop self
      context.become(stopped)
    case Terminated(ref) if ref == self =>
      ()
  }

  val stopped: Receive = {
    case _ =>
      // we're stopped, ignore all the things
  }

  def setupSubscription(channel: Channel): String = {
    channel.basicConsume(queueName, false,
      new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
          self ! Delivery(consumerTag, envelope, properties, body)
        }
      }
    )
  }

  def handleUnsubscribe(channel: Channel, consumerTag: Option[String]): Unit = {
    try {
      consumerTag.foreach(channel.basicCancel(_))
    } catch {
      case RabbitExceptionMatchers.NonFatalRabbitException(_) =>
        ()
    }
  }

  def handleRejectOrAck(ack: Boolean, channel: Channel, consumerTag: Long): Unit = {
    pendingDeliveries.remove(consumerTag)
    if (ack)
      Try { channel.basicAck(consumerTag, false) }
    else
      Try { channel.basicReject(consumerTag, true) }
  }

  def handleDelivery(channel: Channel, delivery: Delivery): Unit = {
    val Delivery(consumerTag, envelope, properties, body) = delivery
    pendingDeliveries.add(envelope.getDeliveryTag)
    import context.dispatcher // we're just going to borrow it for a few milliseconds... we'll give it back, we promise!

    lazy val reportError = rabbitErrorLogging(name, _: String, _: Throwable, consumerTag, envelope, properties, body)

    val handled = Promise[Result]

    Future {
      try handle(handled, delivery)
      catch {
        case e: Throwable =>
          handled.success(Left(UnhandledExceptionRejection("Error while running handler", e)))
      }
    }

    handled.future.
      recover { case e => Left(UnhandledExceptionRejection("Unhandled exception occurred in async acking Future", e)) }.
      flatMap {
        case Right(_) =>
          Future.successful(true)
        case Left(r @ NackRejection(msg)) =>
          Future.successful(false) // just nack the message; it was intentional. Don't recover. Don't report
        case Left(r @ UnhandledExceptionRejection(msg, cause)) =>
          reportError(msg, cause)
          recoveryStrategy(cause, channel, queueName, delivery)
        case Left(r @ ExtractRejection(msg)) =>
          // retrying is not going to do help. What to do? ¯\_(ツ)_/¯
          reportError(s"Could not extract required data", r)
          Future.successful(true) // just nack the message; don't recover
      }.
      onComplete {
        case Success(ack) =>
          self ! RejectOrAck(ack, envelope.getDeliveryTag())
        case Failure(e) =>
          log.error(s"Recovery strategy failed, or something else went horribly wrong", e)
          self ! RejectOrAck(false, envelope.getDeliveryTag())
      }
  }
}
