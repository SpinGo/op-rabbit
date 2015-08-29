package com.spingo.op_rabbit
package consumer

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

/**
  A Subscription combines together a [[Binding]] and a [[Consumer]], where the binding defines how the message queue is declare and if any topic bindings are involved, and the consumer declares how messages are to be consumed from the message queue specified by the [[Binding]]. This object is sent to [[RabbitControl]] to activate.

  It features convenience methods to with Futures to help timing.
  */
class Subscription private(config: BoundChannel) extends Directives {
  // def config: BoundChannel

  lazy val _config = config
  lazy val channelConfiguration = _config.configuration
  lazy val binding = _config.boundSubscription.binding
  lazy val handler = _config.boundSubscription.handler
  lazy val _errorReporting = _config.boundSubscription.errorReporting
  lazy val _recoveryStrategy = _config.boundSubscription.recoveryStrategy
  lazy val _executionContext = _config.boundSubscription.executionContext

  def register(rabbitControl: ActorRef, timeout: FiniteDuration = 5 seconds): SubscriptionRef = {
    implicit val akkaTimeout = Timeout(timeout)
    SubscriptionRefProxy((rabbitControl ? this).mapTo[SubscriptionRef])
  }
}
