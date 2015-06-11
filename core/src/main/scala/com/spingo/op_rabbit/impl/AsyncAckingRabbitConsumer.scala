package com.spingo.op_rabbit.impl

import akka.actor.{Actor, ActorLogging, ActorSystem, Terminated}
import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit._
import com.thenewmotion.akka.rabbitmq.{Channel, DefaultConsumer, Envelope}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


object AsyncAckingRabbitConsumer {
  type RecoveryStrategy = (Throwable, Channel, Envelope, BasicProperties, Array[Byte]) => Future[Unit]
  private val futureUnit: Future[Unit] = Future.successful(Unit)

  def withRetry(queueName: String, redeliverDelay: FiniteDuration = 10 seconds, retryCount: Int = 3)(implicit actorSystem: ActorSystem): RecoveryStrategy = { (ex, channel, envelop, properties, body) =>
    import actorSystem.dispatcher
    val thisRetryCount = PropertyHelpers.getRetryCount(properties)
    if (thisRetryCount < retryCount)
      akka.pattern.after(redeliverDelay, actorSystem.scheduler) {
        val withRetryCountIncremented = PropertyHelpers.setRetryCount(properties, thisRetryCount + 1)
        // we don't need to worry about reliable delivery here; if this fails to publish, then it is because the message was already nacked.
        channel.basicPublish("", queueName, withRetryCountIncremented, body)
        futureUnit
      }
    else
      futureUnit
  }
}

// TODO - implement retry according to this pattern: http://yuserinterface.com/dev/2013/01/08/how-to-schedule-delay-messages-with-rabbitmq-using-a-dead-letter-exchange/
// - create two direct exchanges: work and retry
// - Publish to rety with additional header set/incremented: x-redelivers
// -  { "x-dead-letter-exchange", WORK_EXCHANGE },
// -  { "x-message-ttl", RETRY_DELAY }
// -   (setting since nothing consumes the direct exchange, it will go back to retry queue)
protected [op_rabbit] class AsyncAckingRabbitConsumer[T](
  name: String,
  queueName: String,
  recoveryStrategy: AsyncAckingRabbitConsumer.RecoveryStrategy,
  onChannel: (Channel) => Unit,
  handle: T => Future[Unit])(implicit
    unmarshaller: RabbitUnmarshaller[T],
    rabbitErrorLogging: RabbitErrorLogging) extends Actor with ActorLogging {

  import Consumer._
  private case class PipelineException(msg: String, cause: Throwable)
  private type PipelineEither[T] = Either[PipelineException, T]

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
      sender ! true
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
      sender ! true
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

  private def wrapping[T](msg: String)(fn: => T): PipelineEither[T] =
    try { Right(fn) }
    catch { case e: Throwable => Left(PipelineException(msg, e)) }

  def setupSubscription(channel: Channel): String = {
    println(s"setupSubscription(${channel})")
    onChannel(channel)
    channel.basicConsume(queueName, false,
      new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
          self ! Delivery(consumerTag, envelope, properties, body)
        }
      }
    )
  }

  def handleUnsubscribe(channel: Channel, consumerTag: Option[String]): Unit =
    try {
      consumerTag.foreach(channel.basicCancel(_))
    } catch {
      case RabbitExceptionMatchers.NonFatalRabbitException(_) =>
        ()
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

    val failureOrFuture: PipelineEither[Future[PipelineEither[Unit]]] = for {
      data <- wrapping("Error while unmarshalling message") {
        unmarshaller.unmarshall(body, Option(properties.getContentType), Option(properties.getContentEncoding))
      }.right

      result <- wrapping("Error while initializing Future") {
        handle(data)
      }.right

    } yield
      (result map (Right(_)) recover { case e => Left(PipelineException("Error while processing message", e)) })

    val joined: Future[PipelineEither[Unit]] = failureOrFuture.left.map(e => Future.successful(Left(e))).merge

    val result = joined.flatMap {
      case Right(_) =>
        Future.successful(Unit)
      case Left(PipelineException(msg, cause)) =>
        reportError(msg, cause)
        recoveryStrategy(cause, channel, envelope, properties, body)
    }.onComplete {
      case Success(_) =>
        self ! RejectOrAck(true, envelope.getDeliveryTag())
      case Failure(e) =>
        log.error(s"recovery strategy refused; or something else went wrong", e)
        self ! RejectOrAck(false, envelope.getDeliveryTag())
    }
  }
}
