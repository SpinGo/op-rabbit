package com.spingo.op_rabbit
package consumer

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

object Subscription {
  trait SubscriptionCommands
  case class SetQos(qos: Int) extends SubscriptionCommands
}

trait SubscriptionRef {
  /**
    Future is completed the moment the subscription closes.
    */
  val closed: Future[Unit]

  /**
    Future is completed once the message queue and associated bindings are configured.
    */
  val initialized: Future[Unit]

  /**
    Causes consumer to immediately stop receiving new messages; once pending messages are complete / acknowledged, shut down all associated actors, channels, etc.

    If pending messages aren't complete after the provided timeout (default 5 minutes), the channel is closed and the unacknowledged messages will be scheduled for redelivery.
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
trait Subscription extends Directives with SubscriptionRef {
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

  def register(rabbitControl: ActorRef, timeout: FiniteDuration = 5 seconds): SubscriptionRef = {
    implicit val akkaTimeout = Timeout(timeout)
    SubscriptionRefProxy((rabbitControl ? this).mapTo[SubscriptionRef])
  }
}

case class SubscriptionRefDirect(subscriptionActor: ActorRef, initialized: Future[Unit], closed: Future[Unit]) extends SubscriptionRef {
  def close(timeout: FiniteDuration = 5 minutes): Unit =
    subscriptionActor ! SubscriptionActor.Stop(None, timeout)

  def abort(): Unit =
    subscriptionActor ! SubscriptionActor.Abort(None)
}

case class SubscriptionRefProxy(subscriptionRef: Future[SubscriptionRef]) extends SubscriptionRef {
  implicit val ec = SameThreadExecutionContext
  val closed = subscriptionRef.flatMap(_.closed)
  val initialized = subscriptionRef.flatMap(_.initialized)
  def close(timeout: FiniteDuration = 5 minutes): Unit =
    subscriptionRef.foreach(_.close(timeout))

  def abort(): Unit =
    subscriptionRef.foreach(_.abort())
}
