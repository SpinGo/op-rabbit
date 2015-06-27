package com.spingo.op_rabbit.consumer

import com.spingo.op_rabbit.{Binding, QueueBinding, RabbitControl, RabbitUnmarshaller, TopicBinding}
import com.spingo.op_rabbit.properties.PropertyExtractor
import scala.concurrent.{ExecutionContext, Future, Promise}
import shapeless._

trait Ackable {
  val handler: Handler
}
object Ackable {
  implicit def ackableFromFuture(f: Future[_]) = new Ackable {
    val handler: Handler = { (p, delivery) =>
      // TODO - reuse thread
      import scala.concurrent.ExecutionContext.Implicits.global
      p.completeWith(f.map(_ => Right(Unit)))
    }
  }

  implicit def ackableFromUnit(u: Unit) = new Ackable {
    val handler: Handler = { (p, delivery) =>
      p.success(Right(u))
    }
  }
}

trait Nackable {
  val handler: Handler
}
object Nackable {
  implicit def nackableFromRejection(rejection: Rejection) = new Nackable {
    val handler: Handler = { (p, _) => p.success(Left(rejection)) }
  }
  implicit def nackableFromString(u: String) = new Nackable {
    val handler: Handler = { (p, _) => p.success(Left(NackRejection(u))) }
  }
  implicit def nackableFromUnit(u: Unit) = new Nackable {
    val handler: Handler = { (p, _) => p.success(Left(NackRejection("General Handler Rejection"))) }
  }
}

case class BoundSubscription(binding: Binding, handler: Handler, errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy, executionContext: ExecutionContext)

case class SubscriptionDirective(binding: Binding, errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy, executionContext: ExecutionContext) {
  def apply(thunk: => Handler) =
    BoundSubscription(binding, handler = thunk, errorReporting, recoveryStrategy, executionContext)
}

case class ChannelConfiguration(qos: Int)
case class BoundChannel(configuration: ChannelConfiguration, boundSubscription: BoundSubscription)
case class ChannelDirective(config: ChannelConfiguration) {
  def apply(thunk: => BoundSubscription) = BoundChannel(config, thunk)
}


trait Directives {
  def channel(qos: Int = 1): ChannelDirective = ChannelDirective(ChannelConfiguration(qos))

  def consume(tuple: (Binding, RabbitErrorLogging, RecoveryStrategy, ExecutionContext)) = SubscriptionDirective.tupled(tuple)

  def queue(
    queue: String,
    durable: Boolean = true,
    exclusive: Boolean = false,
    autoDelete: Boolean = false)(implicit errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy, executionContext: ExecutionContext) = (QueueBinding(queue, durable, exclusive, autoDelete), errorReporting, recoveryStrategy, executionContext)

  def topic(
    queue: String,
    topics: List[String],
    exchange: String = RabbitControl topicExchangeName,
    durable: Boolean = true,
    exclusive: Boolean = false,
    autoDelete: Boolean = false,
    exchangeDurable: Boolean = true)(implicit errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy, executionContext: ExecutionContext) = (TopicBinding(queue, topics, exchange, durable, exclusive, autoDelete, exchangeDurable), errorReporting, recoveryStrategy, executionContext)

  def as[T](implicit um: RabbitUnmarshaller[T]) = um

  def provide[T](value: T) = hprovide(value :: HNil)
  def hprovide[T <: HList](value: T) = new Directive[T] {
    def happly(fn: T => Handler) =
      fn(value)
  }
  def ack(f: Ackable): Handler = f.handler
  def nack(f: Nackable): Handler = f.handler

  def body[T](um: RabbitUnmarshaller[T]): Directive1[T] = new Directive1[T] {
    def happly(fn: ::[T, HNil] => Handler): Handler = { (promise, delivery) =>
      val data = um.unmarshall(delivery.body, Option(delivery.properties.getContentType), Option(delivery.properties.getContentEncoding))
      fn(data :: HNil)(promise, delivery)
    }
  }

  def extract[T](map: Delivery => T) = new Directive1[T] {
    def happly(fn: ::[T, HNil] => Handler): Handler = { (promise, delivery) =>
      val data = map(delivery)
      fn(data :: HNil)(promise, delivery)
    }
  }
  def extractEither[T](map: Delivery => Either[Rejection, T]) = new Directive1[T] {
    def happly(fn: ::[T, HNil] => Handler): Handler = { (promise, delivery) =>
      map(delivery) match {
        case Left(rejection) => promise.success(Left(rejection))
        case Right(value) => fn(value :: HNil)(promise, delivery)
      }
    }
  }

  def optionalProperty[T](extractor: PropertyExtractor[T]) = extract { delivery =>
    extractor.unapply(delivery.properties)
  }
  def property[T](extractor: PropertyExtractor[T]) = extractEither { delivery =>
    extractor.unapply(delivery.properties) match {
      case Some(v) => Right(v)
      case None => Left(ExtractRejection(s"Property ${extractor.extractorName} was not provided"))
    }
  }

  def isRedeliver = extract(_.envelope.isRedeliver)
  def exchange = extract(_.envelope.getExchange)
  def routingKey = extract(_.envelope.getRoutingKey)
}
object Directives extends Directives
