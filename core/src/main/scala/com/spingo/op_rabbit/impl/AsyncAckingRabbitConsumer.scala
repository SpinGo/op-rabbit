package com.spingo.op_rabbit
package impl

import akka.actor.{Actor, ActorLogging, Terminated, ActorRef}
import com.rabbitmq.client.AMQP.BasicProperties
import com.thenewmotion.akka.rabbitmq.{Channel, DefaultConsumer, Envelope}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

private [op_rabbit] class AsyncAckingRabbitConsumer[T](
  name: String,
  subscription: BoundConsumerDefinition)(implicit executionContext: ExecutionContext) extends Actor with ActorLogging {

  import Consumer._

  var pendingDeliveries = mutable.Set.empty[Long]

  context watch self

  /**
    Tell the parent actor (SubscriptionActor) about this failure
    */
  def propCause(cause: Option[Throwable]): Unit =
    cause foreach (c => context.parent ! SubscriptionActor.Stop(Some(c)))
  def receive = {
    case Subscribe(channel) =>
      val consumerTag = setupSubscription(channel)
      context.become(connected(channel, Some(consumerTag)))
    case Unsubscribe =>
      ()
    case Abort(cause) =>
      propCause(cause)
      context stop self
    case Shutdown(cause) =>
      propCause(cause)
      context stop self
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
      context.become(connected(channel, None))
    case delivery: Delivery =>
      handleDelivery(channel, delivery)
    case r : AckOrNack =>
      handleAckOrNack(r, channel)
    case Shutdown(cause) =>
      propCause(cause)
      handleUnsubscribe(channel, consumerTag)
      if(pendingDeliveries.isEmpty)
        context stop self
      else
        context.become(stopping(channel))

    case Abort(cause) =>
      propCause(cause)
      context stop self
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
    case r: AckOrNack =>
      handleAckOrNack(r, channel)
      if (pendingDeliveries.isEmpty)
        context stop self
    case Delivery(consumerTag, envelope, properties, body) =>
      // note! Before RabbitMQ 2.7.0 does not preserve message order when this happens!
      // https://www.rabbitmq.com/semantics.html
      channel.basicReject(envelope.getDeliveryTag, true)
    case Unsubscribe | Shutdown(_) =>
      ()
    case Abort(cause) =>
      propCause(cause)
      context stop self
    case Terminated(ref) if ref == self =>
      ()
  }

  def setupSubscription(channel: Channel): String = {
    channel.basicConsume(
      subscription.queue.queueName,
      false,
      properties.toJavaMap(subscription.consumerArgs),
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

  def handleAckOrNack(rejectOrAck: AckOrNack, channel: Channel): Unit = {
    pendingDeliveries.remove(rejectOrAck.deliveryTag)
    rejectOrAck(channel)
  }

  def handleDelivery(channel: Channel, delivery: Delivery): Unit = {
    pendingDeliveries.add(delivery.envelope.getDeliveryTag)

    lazy val reportError = subscription.errorReporting(name, _: String, _: Throwable, delivery.consumerTag, delivery.envelope, delivery.properties, delivery.body)

    def doTheThing(whileText: String)(fn: (Promise[Result], Delivery) => Unit): Future[Result] = {
      val handled = Promise[Result]

      try fn(handled, delivery)
      catch {
        case e: Throwable =>
          handled.trySuccess(Left(UnhandledExceptionRejection(s"Error while ${whileText}", e)))
      }
      handled.future.recover { case e => Left(UnhandledExceptionRejection(s"Unhandled exception occurred while ${whileText}", e)) }
    }

    Future {
      doTheThing("running handler")(subscription.handler)
    }.flatMap(identity).
      flatMap {
        case r @ Right(_) =>
          Future.successful(r)
        case Left(r @ UnhandledExceptionRejection(msg, cause)) =>
          reportError(msg, cause)
          doTheThing("running recoveryStrategy")(subscription.recoveryStrategy(cause, channel, subscription.queue.queueName))
        case Left(r: ExtractRejection) =>
          // retrying is not going to do help. What to do? ¯\_(ツ)_/¯
          reportError(s"Could not extract required data", r)
          doTheThing("running recoveryStrategy")(subscription.recoveryStrategy(r, channel, subscription.queue.queueName))
      }.
      foreach {
        case Right(ackOrNack) =>
          self ! ackOrNack

        case Left(e @ UnhandledExceptionRejection(_, RabbitExceptionMatchers.NonFatalRabbitException(_))) =>
          log.error(s"Some kind of connection issue likely caused our recovery strategy to fail; Nacking with requeue = true.", e)
          self ! Nack(true, delivery.envelope.getDeliveryTag)

        case Left(UnhandledExceptionRejection(_, e)) =>
          log.error(s"Recovery strategy failed, or something else went horribly wrong; Nacking with requeue = true, then shutting consumer down.", e)
          self ! Shutdown(Some(e))
          self ! Nack(true, delivery.envelope.getDeliveryTag)

        case Left(e: ExtractRejection) =>
          log.error(s"Recovery Strategy rejected; Nacking with requeue = true, then shutting consumer down.", e)
          self ! Shutdown(Some(e))
          self ! Nack(true, delivery.envelope.getDeliveryTag)
      }
  }
}
