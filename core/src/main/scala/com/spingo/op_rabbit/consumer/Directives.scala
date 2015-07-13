package com.spingo.op_rabbit.consumer

import com.spingo.op_rabbit.{RabbitControl, RabbitUnmarshaller}
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

  val ackUnitHandler: Handler = { (p, delivery) =>
    p.success(Right(Unit))
  }

  val unitAck = new Ackable {
    val handler: Handler = ackUnitHandler
  }

  implicit def ackableFromUnit(u: Unit) = unitAck
}

class TypeHolder[T] {}
object TypeHolder {
  def apply[T] = new TypeHolder[T]
}

trait Nackable {
  val handler: Handler
}

object Nackable {
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

/**
  Directives power the declarative DSL of op-rabbit.

  In order to define a consumer, you need a [[Directives.channel channel]] directive, a [[Directives.consume consumer]] directive, and one or more extractor directives. For example:

  {{{
  channel(qos = 3) {
    consume(queue("my.queue.name")) {
      (body(as[MyPayloadType]) & optionalProperty(ReplyTo) & optionalProperty(Priority)) { (myPayload, replyTo, priority) =>
        // work ...
        ack()
      }
    }
  }
  }}}
  
  As seen, directives are composable via `&`. In the end, the directive is applied with a function whose parameters match the output values from the directive(s).

  One value the directives, over accessing the properties directly, is that they are type safe, and care was taken to reduce the probability of surprise. Death is swiftly issued to `null` and `Object`. Some directives, such as [[Directives.property property]], will nack the message if the value specified can't be extracted; IE it is null. If you'd prefer to use a default value instead of nacking the message, you can specify alternative values using `| provide(...)`.

  {{{
  (property(ReplyTo) | provide("default-reply-to") { replyTo =>
    // ...
  }
  }}}
  
  Note: the directives themselves don't actually do anything, except when applied / returned. IE:

  {{{
  channel(qos = 3) {
    consume(queue("my.queue.name")) {
      property(ReplyTo) // does absolutely nothing

      body(as[MyPayloadType]) { (myPayload, replyTo, priority) =>
        ack() // does absolutely nothing (not the return value)
        // work ...
        ack() // DOES something
      }
    }
  }
  }}}
  */
trait Directives {
  /**
    Declarative which declares a channel
    */
  def channel(qos: Int = 1): ChannelDirective = ChannelDirective(ChannelConfiguration(qos))

  /**
    Declarative which declares a consumer
    */
  def consume(tuple: (Binding, RabbitErrorLogging, RecoveryStrategy, ExecutionContext)) = SubscriptionDirective.tupled(tuple)

  /**
    Provides values for the [[consume]] directive.
    */
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
  def typeOf[T] = new TypeHolder[T]

  /**
    
    */
  def provide[T](value: T) = hprovide(value :: HNil)
  def hprovide[T <: HList](value: T) = new Directive[T] {
    def happly(fn: T => Handler) =
      fn(value)
  }

  /**
    Ack the message

    Examples:

    {{{
    ack()

    ack(Future {
      //Some work
    })
    }}}

    Note that in the case of acking with a Future, if the Future fails, then the message is counted as erroneous, and the [[RecoveryStrategy]] is use is applied.
    */
  def ack(f: Ackable): Handler = f.handler
  def ack: Handler = Ackable.ackUnitHandler

  /**
    Nack the message; does NOT trigger the [[RecoveryStrategy]] in use.
    
    Examples:

    {{{
    nack()

    nack(Rejection("I can't handle this message right now")
    }}}
    */
  def nack(f: Nackable): Handler = f.handler

  /**
    Extract the message body. Uses a [[com.spingo.op_rabbit.RabbitUnmarshaller RabbitUnmarshaller]] to deserialize.

    Example:

    {{{
    body(as[JobDescription])
    }}}
    */
  def body[T](um: RabbitUnmarshaller[T]): Directive1[T] = new Directive1[T] {
    def happly(fn: ::[T, HNil] => Handler): Handler = { (promise, delivery) =>
      val data = um.unmarshall(delivery.body, Option(delivery.properties.getContentType), Option(delivery.properties.getContentEncoding))
      fn(data :: HNil)(promise, delivery)
    }
  }

  /**
    Extract any arbitrary value from the delivery / Java RabbitMQ objects. Accepts a function which receives a Delivery and returns some value.
    */
  def extract[T](map: Delivery => T) = new Directive1[T] {
    def happly(fn: ::[T, HNil] => Handler): Handler = { (promise, delivery) =>
      val data = map(delivery)
      fn(data :: HNil)(promise, delivery)
    }
  }
  /**
    Like extract, but the provided function should return an Either, with left for a rejection, right for success.
    */
  def extractEither[T](map: Delivery => Either[Rejection, T]) = new Directive1[T] {
    def happly(fn: ::[T, HNil] => Handler): Handler = { (promise, delivery) =>
      map(delivery) match {
        case Left(rejection) => promise.success(Left(rejection))
        case Right(value) => fn(value :: HNil)(promise, delivery)
      }
    }
  }

  /**
    Given a [[com.spingo.op_rabbit.properties property]], yields Some(value). If the underlying value does not exist (is null), then it yields None.
    */
  def optionalProperty[T](extractor: PropertyExtractor[T]) = extract { delivery =>
    extractor.unapply(delivery.properties)
  }

  /**
    Given a [[com.spingo.op_rabbit.properties property]], yields it's value. If the underlying value does not exist (is null), then it nacks.
    */
  def property[T](extractor: PropertyExtractor[T]) = extractEither { delivery =>
    extractor.unapply(delivery.properties) match {
      case Some(v) => Right(v)
      case None => Left(ValueExpectedExtractRejection(s"Property ${extractor.extractorName} was not provided"))
    }
  }

  /**
    Directive which yields whether this message been delivered once before (although, not necessarily received)
    */
  def isRedeliver = extract(_.envelope.isRedeliver)

  /**
    Directive which yields the exchange through which the message was published
    */
  def exchange = extract(_.envelope.getExchange)

  /**
    Directive which yields the routingKey (topic) through which the message was published
    */
  def routingKey = extract(_.envelope.getRoutingKey)
}

/**
  Convenience object and recommended way for bringing the directives in scope. See [[Directives]] trait.
  */
object Directives extends Directives
