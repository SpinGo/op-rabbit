package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.pipe
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.rabbitmq.client.AMQP.BasicProperties
import com.thenewmotion.akka.rabbitmq.Channel
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}

trait MessageForConfirmedPublication extends MessageForPublicationLike {
  val dropIfNoChannel = false
  val published: Future[Unit]
}

object MessageForConfirmedPublication {
  trait Factory {
    def apply[T](message: T)(implicit marshaller: RabbitMarshaller[T]): MessageForConfirmedPublication
  }
  // type Factory[T] = T => MessageForConfirmedPublication
}

// Publishes the message inside of a transaction in order to guarantee that the message is persisted
// About 150x slower than async publishing.
class GuaranteedPublishedMessage(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties) extends MessageForConfirmedPublication {

  private val promise = Promise[Unit]
  val published = promise.future

  def apply(c: Channel): Unit = {
    try {
      c.txSelect()
      publisher(c, data, properties)
      c.txCommit()
      promise.success()
    } catch {
      case e @ RabbitExceptionMatchers.NonFatalRabbitException(_) =>
        throw(e)
      case e: Throwable =>
        promise.failure(e)
        throw(e)
    }
  }
}

// Publishes the message using the provided MessagePublisher; fulfills
// a promise so the submitter can track when the message was actually
// sent off
class NotifyingPublishedMessage(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties) extends MessageForConfirmedPublication {

  private val promise = Promise[Unit]
  val published = promise.future

  def apply(c: Channel): Unit = {
    try {
      publisher(c, data, properties)
      promise.success()
    } catch {
      case e @ RabbitExceptionMatchers.NonFatalRabbitException(_) =>
        throw(e)
      case e: Throwable =>
        promise.failure(e)
        throw(e)
    }
  }
}

object NotifyingPublishedMessage {
  // def factory[T](publisher: MessagePublisher)(implicit marshaller: RabbitMarshaller[T]): MessageForConfirmedPublication.Factory[T] = { message =>
  //   val (bytes, properties) = marshaller.marshallWithProperties(message)
  //   new NotifyingPublishedMessage(publisher, bytes, properties.build)
  // }
}


object GuaranteedPublishedMessage {
  def apply[T](publisher: MessagePublisher) = new MessageForConfirmedPublication.Factory {
    def apply[M](message: M)(implicit marshaller: RabbitMarshaller[M]) = {
      val (bytes, properties) = marshaller.marshallWithProperties(message)
      new GuaranteedPublishedMessage(publisher, bytes, properties.build)
    }
  }
}

object RabbitSink {
  case class ChannelNotAvailable(name: String) extends Exception(s"No channel available for RabbitSink ${name}; outbound message dropped")
  def apply[T](name: String, rabbitControl: ActorRef, messageFactory: MessageForConfirmedPublication.Factory)(implicit marshaller: RabbitMarshaller[T]): Sink[(Promise[Unit], T), Future[Unit]] = {
    Flow[(Promise[Unit], T)].
      map { case (p, payload) =>
        val msg = messageFactory(payload)
        rabbitControl ! msg
        p.completeWith(msg.published)
        p.future
      }.
      mapAsync(8)(identity). // resolving the futures in the stream causes back-pressure in the case of a rabbitMQ connection being unavailable; specifying a number greater than 1 is for buffering
      toMat(Sink.foreach(_ => ()))(Keep.right)
  }
}
