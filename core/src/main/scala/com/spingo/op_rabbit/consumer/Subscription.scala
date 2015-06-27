package com.spingo.op_rabbit.consumer

import akka.actor.ActorRef
import com.spingo.op_rabbit.Binding
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

object Subscription {
  trait SubscriptionCommands
  case class SetQos(qos: Int) extends SubscriptionCommands
}

trait SubscriptionControl {
  /**
    Future is completed the moment the subscription closes.
    */
  val closed: Future[Unit]

  /**
    Future is completed once the graceful shutdown process initiates.
    */
  val closing: Future[Unit]

  /**
    Future is completed once the message queue and associated bindings are configured.
    */
  val initialized: Future[Unit]

  /**
    Causes consumer to immediately stop receiving new messages; once pending messages are complete / acknowledged, shut down all associated actors, channels, etc.

    If pending messages aren't complete after the provided timeout, the channel is closed and the unacknowledged messages will be scheduled for redelivery.
    */
  def close(timeout: FiniteDuration = 5 minutes): Unit

  /**
    Like close, but don't wait for pending messages to finish processing.
    */
  def abort(): Unit
}

/**
  A Subscription combines together a [[Binding]] and a [[Consumer]], where the binding defines how the message queue is declare and if any topic bindings are involved, and the consumer declares how messages are to be consumed from the message queue specified by the [[Binding]]. This object is sent to [[RabbitControl]] to activate.

  It features convenience methods to with Futures to help timing.
  */
trait Subscription extends Directives with SubscriptionControl {
  protected [op_rabbit] val _initializedP = Promise[Unit]

  protected [op_rabbit] val _closedP = Promise[Unit]
  protected [op_rabbit] val _closingP = Promise[FiniteDuration]
  protected [op_rabbit] val _subscriptionRef = Promise[ActorRef]
  protected [op_rabbit] val subscriptionRef = _subscriptionRef.future
  private val _abortingP = Promise[Unit]

  final val aborting = _abortingP.future

  final val closed = _closedP.future
  final val closing: Future[Unit] = _closingP.future.map( _ => () )(ExecutionContext.global)
  final val initialized = _initializedP.future
  final def close(timeout: FiniteDuration = 5 minutes) = _closingP.trySuccess(timeout)
  final def abort() = _abortingP.trySuccess(())

  def config: BoundChannel

  lazy val _config = config
  lazy val channelConfiguration = _config.configuration
  lazy val binding = _config.boundSubscription.binding
  lazy val handler = _config.boundSubscription.handler
  lazy val _errorReporting = _config.boundSubscription.errorReporting
  lazy val _recoveryStrategy = _config.boundSubscription.recoveryStrategy
  lazy val _executionContext = _config.boundSubscription.executionContext
}

/**
  scala
  // stop receiving new messages from RabbitMQ immediately; shut down consumer and channel as soon as pending messages are completed. A grace period of 30 seconds is given, after which the subscription forcefully shuts down.
  subscription.

  // Shut things down without a grace period
  subscription.abort()

  // Future[Unit] which completes once the provided binding has been applied (IE: queue has been created and topic bindings configured). Useful if you need to assert you don't send a message before a message queue is created in which to place it.
  subscription.initialized

  // Future[Unit] which completes when the subscription is closed.
  subscription.closed

  // Future[Unit] which completes when the subscription begins closing.
  subscription.closing
  ```
  */
