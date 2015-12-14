package com.spingo.op_rabbit

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

object Subscription {
  trait SubscriptionCommands
  case class SetQos(qos: Int) extends SubscriptionCommands

  def apply(boundConfig: BoundChannel): Subscription =
    new Subscription(boundConfig)

  def run(rabbitControl: ActorRef)(boundConfig: BoundChannel): SubscriptionRef = {
    new Subscription(boundConfig).run(rabbitControl)
  }
}

object ModeledConsumerArgs {
  import properties._
  /**
    Consumer priorities allow you to ensure that high priority
    consumers receive messages while they are active, with messages
    only going to lower priority consumers when the high priority
    consumers block.

    Normally, active consumers connected to a queue receive messages
    from it in a round-robin fashion. When consumer priorities are in
    use, messages are delivered round-robin if multiple active
    consumers exist with the same high priority.

    [[http://www.rabbitmq.com/consumer-priority.html Read more: Consumer Priority]]
    */
  val `x-priority` = TypedHeader[Int]("x-priority")
}

/**
  A Subscription contains a full definition for a consumer (channel,
  binding, handling, error recovery, reportings, etc.)
  subscription

  This object is sent to [[RabbitControl]] to boot.

  Example instantiation:

  Subscription {
    import Directives._

    channel(qos = 1) {
      consume(queue("such-queue")) {
        body(as[String]) { payload =>
          // do work...
          ack
        }
      }
    }
  }
  */
class Subscription private(config: BoundChannel) extends Directives {
  protected [op_rabbit] lazy val channelConfig = config.channelConfig
  protected [op_rabbit] lazy val queue = config.boundConsumer.queue
  protected [op_rabbit] lazy val consumer = config.boundConsumer

  def run(rabbitControl: ActorRef, timeout: FiniteDuration = 5.seconds): SubscriptionRef = {
    implicit val akkaTimeout = Timeout(timeout)
    SubscriptionRefProxy((rabbitControl ? this).mapTo[SubscriptionRef])
  }
}
