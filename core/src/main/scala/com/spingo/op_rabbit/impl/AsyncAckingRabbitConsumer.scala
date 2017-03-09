package com.spingo.op_rabbit
package impl

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{Actor, ActorLogging, Terminated}
import akka.pattern.pipe
import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.RabbitExceptionMatchers.{ConnectionGoneException, NonFatalRabbitException}
import com.newmotion.akka.rabbitmq.{Channel, DefaultConsumer, Envelope}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, Await}
import scala.concurrent.duration._

private [op_rabbit] object ConsumerId {
  val count = new AtomicInteger()
  def apply() = count.getAndIncrement()
}

private [op_rabbit] class AsyncAckingRabbitConsumer[T](
  name: String,
  subscription: BoundConsumerDefinition,
  handlerExecutionContext: ExecutionContext) extends Actor with ActorLogging {

  import Consumer._

  val pendingDeliveries = mutable.Set.empty[Long]

  context watch self

  /**
    Tell the parent actor (SubscriptionActor) about this failure
    */
  def propCause(cause: Option[Throwable]): Unit =
    cause foreach (c => context.parent ! SubscriptionActor.Stop(Some(c)))

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    propCause(Some(reason))
    super.preRestart(reason, message)
  }

  def receive = {
    case Subscribe(channel, qos) =>
      setupSubscription(channel, qos) foreach { consumerTag =>
        context.become(connected(channel, Some(consumerTag)))
      }
    case Unsubscribe =>
      ()
    case Shutdown(cause) =>
      propCause(cause)
      context stop self
    case Terminated(ref) if ref == self =>
      ()
  }

  def async(handler: Handler)(implicit ec: ExecutionContext): Handler = { (p, delivery) =>
    Future { handler(p, delivery) } onFailure { case ex => p.failure(ex) }
  }

  def connected(channel: Channel, consumerTag: Option[String]): Receive = {
    case Subscription.SetQos(qos) =>
      channel.basicQos(qos)

    case Subscribe(newChannel, qos) =>
      if (channel != newChannel)
        pendingDeliveries.clear()

      setupSubscription(newChannel, qos).foreach { newConsumerTag =>
        context.become(connected(newChannel, Some(newConsumerTag)))
      }

    case Unsubscribe =>
      handleUnsubscribe(channel, consumerTag)
      context.become(connected(channel, None))
    case delivery: Delivery =>
      pendingDeliveries.add(delivery.envelope.getDeliveryTag)
      implicit val ec = handlerExecutionContext
      applyHandler("running handler", delivery)(async(subscription.handler)) pipeTo self

    case r : ReceiveResult =>
      handleAckOrNack(r, channel)

    case Shutdown(cause) =>
      propCause(cause)
      handleUnsubscribe(channel, consumerTag)
      if(pendingDeliveries.isEmpty)
        context stop self
      else
        context.become(stopping(channel))

    case Terminated(ref) if ref == self =>
      handleUnsubscribe(channel, consumerTag)
      context.become(stopping(channel))
  }

  def stopping(channel: Channel): Receive = {
    case Subscribe(newChannel, _) =>
      // we lost our connection while stopping? Just bail. Nothing more to do.
      if (newChannel != channel) {
        pendingDeliveries.clear
        context stop self
      }
    case r: ReceiveResult =>
      handleAckOrNack(r, channel)
      if (pendingDeliveries.isEmpty)
        context stop self
    case Delivery(consumerTag, envelope, properties, body) =>
      // note! Before RabbitMQ 2.7.0 does not preserve message order when this happens!
      // https://www.rabbitmq.com/semantics.html
      channel.basicReject(envelope.getDeliveryTag, true)
    case Unsubscribe | Shutdown(_) =>
      ()
    case Terminated(ref) if ref == self =>
      ()
  }

  def setupSubscription(channel: Channel, initialQos: Int): Option[String] = try {
    channel.basicQos(initialQos)
    Some(channel.basicConsume(
      subscription.queue.queueName,
      false,
      subscription.consumerTagPrefix.fold("")(prefix => s"$prefix-${ConsumerId()}"),
      false,
      false,
      properties.toJavaMap(subscription.consumerArgs),
      new DefaultConsumer(channel) {
        override def handleDelivery(
          consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
          self ! Delivery(consumerTag, envelope, properties, body)
        }
      }
    ))
  } catch {
    case ConnectionGoneException(_) =>
      None
  }


  def handleUnsubscribe(channel: Channel, consumerTag: Option[String]): Unit = {
    try {
      consumerTag.foreach(channel.basicCancel(_))
    } catch {
      case NonFatalRabbitException(_) =>
        ()
    }
  }

  def handleAckOrNack(rejectOrAck: ReceiveResult, channel: Channel): Unit = try {
    val deliveryTag = rejectOrAck.deliveryTag

    if (!(pendingDeliveries contains deliveryTag)) {
      /* if deliveryTag not in pendingDeliveries, this means we've already
       * restarted the actor due to some unhandled exception, the channel was
       * closed and therefore this deliveryTag invalid */
      return ()
    }

    rejectOrAck match {
      case ack: ReceiveResult.Ack =>
        pendingDeliveries.remove(deliveryTag)
        channel.basicAck(deliveryTag, false)
      case nack: ReceiveResult.Nack =>
        pendingDeliveries.remove(deliveryTag)
        channel.basicReject(deliveryTag, nack.requeue)
      case fail: ReceiveResult.Fail =>
        subscription.errorReporting(name,
          "exception while processing message",
          fail.exception,
          fail.delivery.consumerTag, fail.delivery.envelope, fail.delivery.properties, fail.delivery.body)

        val nextStep: ReceiveResult.Success = {
          /* We wait for the result synchronously just in case the recoveryStrategy does asynchronous work. Channel
           * operations are not thread safe. We may wish to warn if a handler crosses an async boundary. */
          try
            Await.result(
              applyHandler("running recoveryStrategy", fail.delivery)(
                subscription.recoveryStrategy(subscription.queue.queueName, channel, fail.exception)),
              5.minutes)
          catch { case ex: Throwable =>
            ReceiveResult.Fail(fail.delivery, Some("exception while running recoveryStrategy"), ex) }
        } match {
          case ackOrNack: ReceiveResult.Success =>
            ackOrNack
          case ReceiveResult.Fail(_, _, e @ NonFatalRabbitException(_)) =>
            log.error(e,
              "Some kind of connection issue likely caused our recovery strategy to fail; Nacking with requeue = true.")

            ReceiveResult.Nack(deliveryTag, true)
          case ReceiveResult.Fail(_, _, exception) =>
            log.error(exception,
              "Recovery strategy failed, or something else went horribly wrong; " +
                "Nacking with requeue = true, then shutting consumer down.")
            self ! Shutdown(Some(exception))
            ReceiveResult.Nack(deliveryTag, true)
        }
        handleAckOrNack(nextStep, channel)
    }
  } catch {
    case NonFatalRabbitException(e) =>
      // Ignore
      ()
  }

  /**
    * Applies the providen handler; unhandled exceptions are caught and recovered as a Fail AckOfNack type.
    */
  private def applyHandler(whileText: String, delivery: Delivery)(fn: Handler): Future[ReceiveResult] = {
    val handled = Promise[ReceiveResult]

    try fn(handled, delivery)
    catch {
      case e: Throwable =>
        handled.trySuccess(ReceiveResult.Fail(delivery, Some(s"Error while ${whileText}"), e))
    }
    handled.future.recover { case e =>
      ReceiveResult.Fail(delivery, Some(s"Unhandled exception occurred while ${whileText}"), e)
    }(context.dispatcher)
  }
}
