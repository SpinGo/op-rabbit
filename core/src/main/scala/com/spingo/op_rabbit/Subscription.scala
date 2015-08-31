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

  def register(rabbitControl: ActorRef)(boundConfig: BoundChannel): SubscriptionRef = {
    new Subscription(boundConfig).register(rabbitControl)
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
  A Subscription combines together a [[Binding]] and a [[Consumer]], where the binding defines how the message queue is declare and if any topic bindings are involved, and the consumer declares how messages are to be consumed from the message queue specified by the [[Binding]]. This object is sent to [[RabbitControl]] to activate.
  */
class Subscription private(config: BoundChannel) extends Directives {
  protected [op_rabbit] lazy val channelConfig = config.channelConfig
  protected [op_rabbit] lazy val queue = config.boundConsumer.queue
  protected [op_rabbit] lazy val consumer = config.boundConsumer

  def register(rabbitControl: ActorRef, timeout: FiniteDuration = 5 seconds): SubscriptionRef = {
    implicit val akkaTimeout = Timeout(timeout)
    SubscriptionRefProxy((rabbitControl ? this).mapTo[SubscriptionRef])
  }
}
