package com.spingo.op_rabbit

import akka.actor._
import akka.pattern.pipe
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.rabbitmq.client.AMQP.BasicProperties
import com.thenewmotion.akka.rabbitmq.Channel
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.{Future, Promise}

object RabbitSink {
  case class ChannelNotAvailable(name: String) extends Exception(s"No channel available for RabbitSink ${name}; outbound message dropped")
  def apply[T](name: String, rabbitControl: ActorRef, messageFactory: MessageForPublicationLike.Factory[T, ConfirmedMessage]): Sink[(Promise[Unit], T), Future[Unit]] = {
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
