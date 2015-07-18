package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.{pipe,ask}
import akka.stream.Graph
import akka.stream.SinkShape
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.rabbitmq.client.AMQP.BasicProperties
import com.thenewmotion.akka.rabbitmq.Channel
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

// WARNING!!! Don't block inside of Runnable (Future) that uses this.
private[op_rabbit] object SameThreadExecutionContext extends ExecutionContext {
  def execute(r: Runnable): Unit =
    r.run()
  override def reportFailure(t: Throwable): Unit =
    throw new IllegalStateException("problem in op_rabbit internal callback", t)
}

// Simply a container class which signals "this is safe to use for acknowledgement"
case class AckedSink[-In, +Mat](akkaSink: Graph[SinkShape[(Promise[Unit], In)], Mat])

case object MessageNacked extends Exception(s"A published message was nacked by the broker.")

object AckedSink {
  import consumer.RabbitFlowHelpers.propException
  def foreach[T](fn: T => Unit) = AckedSink[T, Future[Unit]] {
    Sink.foreach { case (p, data) =>
      propException(p) { fn(data) }
      p.success(())
    }
  }

  def fold[U, T](zero: U)(fn: (U, T) => U) = AckedSink[T, Future[U]] {
    Sink.fold(zero) { case (u, (p, out)) =>
      val result = propException(p) { fn(u, out) }
      p.success(())
      result
    }
  }

  def ack[T] = AckedSink[T, Future[Unit]] {
    Sink.foreach { case (p, data) =>
      p.success(())
    }
  }

  /**
    @param timeoutAfter The duration for which we'll wait for a message to be acked; note, timeouts and non-acknowledged messages will cause the Sink to throw an exception.
    */
  def confirmedPublisher[T](name: String, rabbitControl: ActorRef, messageFactory: MessageForPublicationLike.Factory[T, ConfirmedMessage], timeoutAfter: FiniteDuration = 30 seconds, qos: Int = 8): AckedSink[T, Future[Unit]] = AckedSink {
    implicit val akkaTimeout = akka.util.Timeout(timeoutAfter)
    Flow[(Promise[Unit], T)].
      map { case (p, payload) =>
        val msg = messageFactory(payload)

        implicit val ec = SameThreadExecutionContext
        val acked = (rabbitControl ? msg).mapTo[Boolean] flatMap { a =>
          if (a)
            Future.successful(())
          else
            Future.failed(MessageNacked)
        }
        p.completeWith(acked)
        acked
      }.
      mapAsync(qos)(identity). // resolving the futures in the stream causes back-pressure in the case of a rabbitMQ connection being unavailable; specifying a number greater than 1 is for buffering
      toMat(Sink.foreach { _ =>
        ()
      })(Keep.right)
  }
}

object PublisherSink {
  // Publishes messages to RabbitMQ.
  def apply[T](name: String, rabbitControl: ActorRef, messageFactory: MessageForPublicationLike.Factory[T, ConfirmedMessage], timeoutAfter: FiniteDuration = 30 seconds, qos: Int = 8): Sink[T, Future[Unit]] = {
    implicit val akkaTimeout = akka.util.Timeout(timeoutAfter)
    Flow[T].
      mapAsync(qos) { case (payload) =>
        val msg = messageFactory(payload)

        implicit val ec = SameThreadExecutionContext
          (rabbitControl ? msg).mapTo[Boolean] flatMap { a =>
            if (a)
              Future.successful(())
            else
              Future.failed(MessageNacked)
          }
      }.
      toMat(Sink.foreach { _ => ()})(Keep.right)
  }
}
