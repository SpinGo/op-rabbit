package com.spingo.op_rabbit

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
      p.completeWith(f.map(_ => Right(Ack(delivery.envelope.getDeliveryTag))))
    }
  }

  val ackHandler: Handler = { (p, delivery) =>
    p.success(Right(Ack(delivery.envelope.getDeliveryTag)))
  }

  private [op_rabbit] val nackNoRequeueHandler: Handler = { (p, delivery) =>
    p.success(Right(Nack(false, delivery.envelope.getDeliveryTag)))
  }
  private [op_rabbit] val nackRequeueHandler: Handler = { (p, delivery) =>
    p.success(Right(Nack(true, delivery.envelope.getDeliveryTag)))
  }

  val unitAck = new Ackable {
    val handler: Handler = ackHandler
  }

  implicit def ackableFromUnit(u: Unit) = unitAck
}

protected class TypeHolder[T] {}
protected object TypeHolder {
  def apply[T] = new TypeHolder[T]
}

private [op_rabbit] case class BoundConsumerDefinition(queue: QueueDefinition, handler: Handler, errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy, executionContext: ExecutionContext, consumerArgs: Seq[properties.Header])
private [op_rabbit] case class BindingDirective(binding: QueueDefinition, args: Seq[properties.Header]) {
  def apply(thunk: => Handler)(implicit errorReporting: RabbitErrorLogging, recoveryStrategy: RecoveryStrategy, executionContext: ExecutionContext) =
    BoundConsumerDefinition(binding, handler = thunk, errorReporting, recoveryStrategy, executionContext, args)
}
private [op_rabbit] case class ChannelConfiguration(qos: Int)
private [op_rabbit] case class BoundChannel(channelConfig: ChannelConfiguration, boundConsumer: BoundConsumerDefinition)
private [op_rabbit] case class ChannelDirective(config: ChannelConfiguration) {
  def apply(thunk: => BoundConsumerDefinition) = BoundChannel(config, thunk)
}

/**
  Directives power the declarative DSL of op-rabbit.

  In order to define a consumer, you need a [[Directives.channel channel]] directive, a [[Directives.consume consumer]] directive, and one or more extractor directives. For example:

  {{{
  channel(qos = 3) {
    consume(queue("my.queue.name")) {
      (body(as[MyPayloadType]) & optionalProperty(ReplyTo) & optionalProperty(Priority)) { (myPayload, replyTo, priority) =>
        // work ...
        ack
      }
    }
  }
  }}}

  As seen, directives are composable via `&`. In the end, the directive is applied with a function whose parameters match the output values from the directive(s).

  One value of the directives, as opposed to accessing the AMQP properties directly, is that they are type safe, and care was taken to reduce the probability of surprise. Death is swiftly issued to `null` and `Object`. Some directives, such as [[Directives.property property]], will nack the message if the value specified can't be extracted; IE it is null. If you'd prefer to use a default value instead of nacking the message, you can specify alternative values using `| provide(...)`.

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
        ack // this ack here does absolutely nothing (not the return value)
        // work ...
        ack
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
  def consume(binding: QueueDefinition, args: Seq[properties.Header] = Seq()) = BindingDirective(binding, args)

  /**
    Provides values for the [[consume]] directive.
    */
  def queue(
    queue: String,
    durable: Boolean = true,
    exclusive: Boolean = false,
    autoDelete: Boolean = false) = Queue(queue, durable, exclusive, autoDelete)

  /**
   * Passive queue binding
   */
  def pqueue(queue: String) =
    Queue.passive(queue)


  def topic(
    queue: Queue,
    topics: List[String],
    exchange: Exchange[Exchange.Topic.type] = Exchange.topic(RabbitControl topicExchangeName)) = TopicBinding(queue, topics, exchange)

  /**
   * Passive topic binding
   */
  def passive(queue: Queue): QueueDefinition =
    Queue.passive(queue)

  def passive[T <: Exchange.Value](exchange: Exchange[T]): Exchange[T] =
    Exchange.passive(exchange)

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
  def ack: Handler = Ackable.ackHandler

  /**
    Nack the message; does NOT trigger the [[RecoveryStrategy]] in use.
    */
  def nack(requeue: Boolean): Handler = if (requeue) Ackable.nackRequeueHandler else Ackable.nackNoRequeueHandler

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
    extractor.extract(delivery.properties)
  }

  /**
    Given a [[com.spingo.op_rabbit.properties property]], yields it's value. If the underlying value does not exist (is null), then it nacks.
    */
  def property[T](extractor: PropertyExtractor[T]) = extractEither { delivery =>
    extractor.extract(delivery.properties) match {
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
