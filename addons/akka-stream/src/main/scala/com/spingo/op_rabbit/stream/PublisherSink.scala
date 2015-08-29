package com.spingo.op_rabbit
package stream

import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Keep, Sink}
import scala.concurrent.Future
import scala.concurrent.duration._

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
      toMat(Sink.ignore)(Keep.right)
  }
}
