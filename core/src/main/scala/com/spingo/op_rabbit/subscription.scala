package com.spingo.op_rabbit

import akka.actor.ActorRef
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

/**
  A Subscription combines together a [[Binding]] and a [[Consumer]], where the binding defines how the message queue is declare and if any topic bindings are involved, and the consumer declares how messages are to be consumed from the message queue specified by the [[Binding]]. This object is sent to [[RabbitControl]] to activate.

  It features convenience methods to with Futures to help timing.
  */
case class Subscription[+C <: Consumer](binding: Binding, consumer: C) {
  private [op_rabbit] val _initializedP = Promise[Unit]

  private [op_rabbit] val _closedP = Promise[Unit]
  private [op_rabbit] val _closingP = Promise[FiniteDuration]
  private [op_rabbit] val _consumerRef = Promise[ActorRef]
  protected [op_rabbit] val consumerRef = _consumerRef.future
  private val _abortingP = Promise[Unit]
  /**
    Future is completed the moment the subscription closes.
    */
  val closed = _closedP.future
  val aborting = _abortingP.future

  /**
    Future is completed once the graceful shutdown process initiates.
    */
  val closing: Future[Unit] = _closingP.future.map( _ => () )(ExecutionContext.global)

  /**
    Future is completed once the message queue and associated bindings are configured.
    */
  val initialized = _initializedP.future

  /**
    Causes consumer to immediately stop receiving new messages; once pending messages are complete / acknowledged, shut down all associated actors, channels, etc.
    
    If pending messages aren't complete after the provided timeout, the channel is closed and the unacknowledged messages will be scheduled for redelivery.
    */
  def close(timeout: FiniteDuration = 5 minutes) =
    _closingP.trySuccess(timeout)

  /**
    Like close, but don't wait for pending messages to finish processing.
    */
  def abort() =
    _abortingP.trySuccess(())
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
