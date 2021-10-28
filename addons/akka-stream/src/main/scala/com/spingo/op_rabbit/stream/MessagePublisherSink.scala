package com.spingo.op_rabbit
package stream

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Try,Success,Failure}

import akka.actor.{ActorRef,Props}
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.ask
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{AbstractInHandler, GraphStage, GraphStageLogic}

import com.timcharper.acked.{AckTup, AckedSink}
import akka.stream.scaladsl.Flow
import akka.actor.typed.Behavior
import akka.actor.ActorSystem
import akka.actor.Actor

object MessagePublisherSink {
  private type In = AckTup[Message]

  def acked(name: String, rabbitControl: ActorRef, actorSystem: ActorSystem) = AckedSink {
    MessagePublisherSink(rabbitControl, actorSystem).named(name)
  }
}


case class MessagePublisherSink(rabbitControl: ActorRef, actorSystem: ActorSystem) extends GraphStage[SinkShape[AckTup[Message]]] {
  import MessagePublisherSink.In

  val in: Inlet[In] = Inlet.create("MessagePublisherSinkActor.in")

  override val shape: SinkShape[In] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes) =
    new GraphStageLogic(shape) {
      val queue = scala.collection.mutable.Map.empty[Long, Promise[Unit]]
      val completed = Promise[Unit]

      class ConfirmationActor extends Actor {
        override def receive: Receive = {
            case Message.Ack(id) =>
              queue.remove(id).get.success(())
            case Message.Nack(id) =>
              queue.remove(id).get.failure(new MessageNacked(id))
            case Message.Fail(id, exception: Throwable) =>
              queue.remove(id).get.failure(exception)
        }
      }

      val confirmationActor = actorSystem.actorOf(Props[ConfirmationActor])

      setHandler(in, new AbstractInHandler() {
        override def onPush() {
          val (promise, msg) = grab(in)
          queue(msg.id) = promise
          rabbitControl.tell(msg, sender = confirmationActor)
        }

        override def onUpstreamFinish() {
          completed.success(())
          super.onUpstreamFinish()
        }
      })
    }
}
