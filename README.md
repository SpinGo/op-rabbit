# Op-Rabbit

##### An opinionated RabbitMQ library for Scala and Akka.

# Documentation

Browse the latest [API Docs](https://op-rabbit.github.io/docs/index.html) online.

Issues go here; questions can be posed there as well. Please see the
[Cookbook](https://github.com/SpinGo/op-rabbit/wiki/Cookbook), first.

- https://github.com/SpinGo/op-rabbit/issues

# Intro

Op-Rabbit is a high-level, type-safe, opinionated, composable,
fault-tolerant library for interacting with RabbitMQ; the following is
a high-level feature list:

- Recovery:
    - Consumers automatically reconnect and subscribe if the
      connection is lost
    - Messages published will wait for a connection to be available
- Integration
    - Connection settings pulled from Typesafe config library
    - Asynchronous, concurrent consumption using Scala native Futures
      or the new Akka Streams project.
    - Common pattern for serialization allows easy integration with
      serialization libraries such play-json or json4s
    - Common pattern for exception handling to publish errors to
      Airbrake, Syslog, or all of the above
- Modular
    - Composition favored over inheritance enabling flexible and high
      code reuse.
- Modeled
    - Queue binding, exchange binding modeled with case classes
    - Queue, and Exchange arguments, such as `x-ttl`, are modeled
    - HeaderValues are modeled; if you try and provide RabbitMQ an
      invalid type for a header value, the compiler will let you know.
    - Publishing mechanisms also modeled
- Reliability
    - Builds on the excellent
      [Akka RabbitMQ client](https://github.com/thenewmotion/akka-rabbitmq)
      library for easy recovery.
    - Built-in consumer error recovery strategy in which messages are
      re-delivered to the message queue and retried (not implemented
      for akka-streams integration as retry mechanism affects message
      order)
    - With a single message, pause all consumers if service health
      check fails (IE: database unavailable); easily resume the same.
- Graceful shutdown
    - Consumers and streams can immediately unsubscribe, but stay
      alive long enough to wait for any messages to finish being
      processed.
- Program at multiple levels of abstraction
    - If op-rabbit doesn't do what you need it to, you can either
      extend op-rabbit or interact directly with `akka-rabbitmq`
      [Akka RabbitMQ client](https://github.com/thenewmotion/akka-rabbitmq).
- Tested
    - Extensive integration tests

## Installation

Op-Rabbit is available on Maven Central

```scala
val opRabbitVersion = "2.1.0"

libraryDependencies ++= Seq(
  "com.spingo" %% "op-rabbit-core"        % opRabbitVersion,
  "com.spingo" %% "op-rabbit-play-json"   % opRabbitVersion,
  "com.spingo" %% "op-rabbit-json4s"      % opRabbitVersion,
  "com.spingo" %% "op-rabbit-airbrake"    % opRabbitVersion,
  "com.spingo" %% "op-rabbit-akka-stream" % opRabbitVersion
)
```

### Scala Version Compatibility Matrix:

#### op-rabbit 2.1.x

Supports Scala 2.12 and Scala 2.11.

| module                       | dependsOn                | version   |
| ---------------------------- | ------------------------ | --------- |
| op-rabbit-core               | akka                     | 2.5.x     |
|                              | akka-rabbitmq            | 5.0.x     |
|                              | shapeless                | 2.3.x     |
|                              | type-safe config         | 1.3.x     |
| op-rabbit-play-json          | play-json                | 2.6.x     |
| op-rabbit-json4s             | json4s                   | 3.5.x     |
| op-rabbit-circe              | circe                    | 0.9.x     |
| op-rabbit-airbrake           | airbrake                 | 2.2.x     |
| op-rabbit-akka-stream        | acked-stream             | 2.1.x     |

#### op-rabbit 2.0.x

Supports Scala 2.12 and Scala 2.11.

| module                       | dependsOn                | version   |
| ---------------------------- | ------------------------ | --------- |
| op-rabbit-core               | akka                     | ~> 2.4.17 |
|                              | akka-rabbitmq            | 4.0       |
|                              | shapeless                | ~> 2.3.2  |
|                              | type-safe config         | >= 1.3.0  |
| op-rabbit-play-json          | play-json                | 2.6.0-M5  |
| op-rabbit-json4s             | json4s                   | 3.5.x     |
| op-rabbit-circe              | circe                    | 0.7.x     |
| op-rabbit-airbrake           | airbrake                 | 2.2.x     |
| op-rabbit-akka-stream        | acked-stream             | 2.1.x     |

#### op-rabbit 1.6.x

Supports Scala 2.11 only

| module                       | dependsOn                | version  |
| ---------------------------- | ------------------------ | -------- |
| op-rabbit-core               | akka                     | ~> 2.4.2 |
|                              | akka-rabbitmq            | 2.3      |
|                              | shapeless                | 2.3.x    |
|                              | type-safe config         | >= 1.3.0 |
| op-rabbit-play-json          | play-json                | 2.5.x    |
| op-rabbit-json4s             | json4s                   | 3.4.x    |
| op-rabbit-circe              | circe                    | 0.5.x    |
| op-rabbit-airbrake           | airbrake                 | 2.2.x    |
| op-rabbit-akka-stream        | acked-stream             | 2.1.x    |

## A high-level overview of the available components:

- `op-rabbit-core` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html)
    - Implements basic patterns for serialization and message
      processing.
- `op-rabbit-play-json` [API](https://op-rabbit.github.io/docs/index.html#com.spingo.op_rabbit.PlayJsonSupport$)
    - Easily use
      [Play Json](https://www.playframework.com/documentation/2.4.x/ScalaJson)
      formats to publish or consume messages; automatically sets
      RabbitMQ message headers to indicate content type.
- `op-rabbit-json4s` [API](https://op-rabbit.github.io/docs/index.html#com.spingo.op_rabbit.Json4sSupport$)
    - Easily use [Json4s](http://json4s.org) to serialization
      messages; automatically sets RabbitMQ message headers to
      indicate content type.
- `op-rabbit-airbrake` [API](https://op-rabbit.github.io/docs/index.html#com.spingo.op_rabbit.AirbrakeLogger)
    - Report consumer exceptions to airbrake, using the
      [Airbrake](https://github.com/airbrake/airbrake-java) Java
      library.
- `op-rabbit-akka-stream` [API](https://op-rabbit.github.io/docs/index.html#com.spingo.op_rabbit.stream.package)
    - Process or publish messages using akka-stream.

## Upgrade Guide

Refer to
[Upgrade Guide wiki page](https://github.com/SpinGo/op-rabbit/wiki/Upgrading)
for help upgrading.

## Usage

Set up RabbitMQ connection information in `application.conf`:

```conf
op-rabbit {
  topic-exchange-name = "amq.topic"
  channel-dispatcher = "op-rabbit.default-channel-dispatcher"
  default-channel-dispatcher {
    # Dispatcher is the name of the event-based dispatcher
    type = Dispatcher

    # What kind of ExecutionService to use
    executor = "fork-join-executor"

    # Configuration for the fork join pool
    fork-join-executor {
      # Min number of threads to cap factor-based parallelism number to
      parallelism-min = 2

      # Parallelism (threads) ... ceil(available processors * factor)
      parallelism-factor = 2.0

      # Max number of threads to cap factor-based parallelism number to
      parallelism-max = 4
    }
    # Throughput defines the maximum number of messages to be
    # processed per actor before the thread jumps to the next actor.
    # Set to 1 for as fair as possible.
    throughput = 100
  }
  connection {
    virtual-host = "/"
    hosts = ["127.0.0.1"]
    username = "guest"
    password = "guest"
    port = 5672
    ssl = false
    connection-timeout = 3s
  }
}
```

Note that hosts is an array; Connection attempts will be made to hosts
in that order, with a default timeout of `3s`. This way you can
specify addresses of your rabbitMQ cluster, and if one of the
instances goes down, your application will automatically reconnect to
another member of the cluster.

`topic-exchange-name` is the default topic exchange to use; this can
be overriden by passing `exchange = "my-topic"` to
[TopicBinding](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.TopicBinding)
or
[Message.topic](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Message$).

Boot up the RabbitMQ control actor:

```scala
import com.spingo.op_rabbit.RabbitControl
import akka.actor.{ActorSystem, Props}

implicit val actorSystem = ActorSystem("such-system")
val rabbitControl = actorSystem.actorOf(Props[RabbitControl])
```

### Set up a consumer: (Topic subscription)

(this example uses `op-rabbit-play-json`)

```scala
import com.spingo.op_rabbit.PlayJsonSupport._
import com.spingo.op_rabbit._
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global
case class Person(name: String, age: Int)
// setup play-json serializer
implicit val personFormat = Json.format[Person]
implicit val recoveryStrategy = RecoveryStrategy.none

val subscriptionRef = Subscription.run(rabbitControl) {
  import Directives._
  // A qos of 3 will cause up to 3 concurrent messages to be processed at any given time.
  channel(qos = 3) {
    consume(topic(queue("such-message-queue"), List("some-topic.#"))) {
      (body(as[Person]) & routingKey) { (person, key) =>
        /* do work; this body is executed in a separate thread, as
           provided by the implicit execution context */
        println(s"""A person named '${person.name}' with age
          ${person.age} was received over '${key}'.""")
        ack
      }
    }
  }
}
```

Now, test the consumer by sending a message:

```
subscriptionRef.initialized.foreach { _ =>
  rabbitControl ! Message.topic(
    Person("Your name here", 33), "some-topic.cool")
}
```

Stop the consumer:

```
subscriptionRef.close()
```

Note, if your call generates an additional future, you can pass it to
ack, and message will be acked based off the Future success, and
nacked with Failure (such that the configured
[RecoveryStrategy](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.RecoveryStrategy)
if the Future fails:

```scala
  // ...
      (body(as[Person]) & routingKey) { (person, key) =>
        /* do work; this body is executed in a separate thread, as
           provided by the implicit execution context */
        val result: Future[Unit] = myApi.methodCall(person)
        ack(result)
      }
  // ...

```
#### Consuming from existing queues
If the queue already exists and doesn't match the expected configuration, topic subscription will fail. To bind to an externally configured queue use `Queue.passive`:

```scala
  channel(qos = 3) {
    consume(Queue.passive("very-exist-queue")) { ...
```

It is also possible to optionally create the queue if it doesn't exist, by providing a `QueueDefinition` instead of a `String`:

```scala
  channel(qos = 3) {
    consume(Queue.passive(topic(queue("wow-maybe-queue"), List("some-topic.#")))) { ...
```
#### Accessing additional headers

As seen in the example above, you can extract headers in addition to
the message body, using op-rabbit's
[Directives](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directives). You
can use multiple declaratives via multiple nested functions, as
follows:

```scala
import com.spingo.op_rabbit.properties._

// Nested directives
// ...
      body(as[Person]) { person =>
        optionalProperty(ReplyTo) { replyTo =>
          // do work
          ack
        }
      }
// ...
```

Or, you can combine directives using `&` to form a compound directive, as follows:

```scala
// Compound directive
// ...
      (body(as[Person]) & optionalProperty(ReplyTo)) { (person, replyTo) =>
        // do work
        ack
      }
// ...
```

See the documentation on [Directives](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.Directives) for more details.

#### Shutting down a consumer

The following methods are available on a [SubscriptionRef](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.SubscriptionRef) which will allow control over the subscription.

```scala
/* stop receiving new messages from RabbitMQ immediately; shut down
   consumer and channel as soon as pending messages are completed. A
   grace period of 30 seconds is given, after which the subscription
   forcefully shuts down. (Default of 5 minutes used if duration not
   provided) */
subscription.close(30 seconds)

/* Shut down the subscription immediately; don't wait for messages to
   finish processing. */
subscription.abort()

/* Future[Unit] which completes once the provided binding has been
   applied (IE: queue has been created and topic bindings
   configured). Useful if you need to assert you don't send a message
   before a message queue is created in which to place it. */
subscription.initialized

// Future[Unit] which completes when the subscription is closed.
subscription.closed
```

#### Recovery strategy:

A recovery strategy defines how a subscription should handle exceptions and **must be provided**. Should it redeliver
them a limited number of times? Or, should it drop them? Several pre-defined recovery strategies with their
corresponding documentation are defined in the
[RecoveryStrategy](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/current/index.html#com.spingo.op_rabbit.RecoveryStrategy$)
companion object.

```
implicit val recoveryStrategy = RecoveryStrategy.nack()
```

### Publish a message:

```scala
rabbitControl ! Message.topic(
  Person(name = "Mike How", age = 33),
  routingKey = "some-topic.very-interest")

rabbitControl ! Message.queue(
  Person(name = "Ivanah Tinkle", age = 25),
  queue = "such-message-queue")
```

By default:

- Messages will be queued up until a connection is available
- Messages are monitored via publisherConfirms; if a connection is
  lost before RabbitMQ confirms receipt of the message, then the
  message is published again. This means that the message may be
  delivered twice, the default opinion being that `at-least-once` is
  better than `at-most-once`. You can use
  [UnconfirmedMessage](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.UnconfirmedMessage)
  if you'd like `at-most-once` delivery, instead.
- If you would like to be notified of confirmation, use the
  [ask](http://doc.akka.io/docs/akka/2.3.12/scala/actors.html#Send_messages)
  pattern:

  ```scala
  import akka.pattern.ask
  import akka.util.Timeout
  import scala.concurrent.duration._
  implicit val timeout = Timeout(5 seconds)
  val received = (
    rabbitControl ? Message.queue(
      Person(name = "Ivanah Tinkle", age = 25),
      queue = "such-message-queue")
  ).mapTo[ConfirmResponse]
  ```

### Consuming using Akka streams

(this example uses `op-rabbit-play-json` and `op-rabbit-akka-streams`)

```scala

import Directives._
implicit val recoveryStrategy = RecoveryStrategy.drop()
RabbitSource(
  rabbitControl,
  channel(qos = 3),
  consume(queue(
    "such-queue",
    durable = true,
    exclusive = false,
    autoDelete = false)),
  body(as[Person])). // marshalling is automatically hooked up using implicits
  runForeach { person =>
    greet(person)
  } // after each successful iteration the message is acknowledged.
```

Note: `RabbitSource` yields an
[AckedSource](https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedSource.scala),
which can be combined with an
[AckedFlow](https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedFlowOps.scala#L519)
and an
[AckedSink](https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedSink.scala)
(such as
[`MessagePublisherSink`](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/akka-stream/current/index.html#com.spingo.op_rabbit.stream.MessagePublisherSink$)). You
can convert an acked stream into a normal stream by calling
`AckedStream.acked`; once messages flow passed the `acked` component,
they are considered acknowledged, and acknowledgement tracking is no
longer a concern (and thus, you are free to use the akka-stream
library in its entirety).

#### Stream failures and recovery strategies

When using the DSL as described in the [consumer setup](https://github.com/SpinGo/op-rabbit#set-up-a-consumer-topic-subscription)
section, recovery strategies are triggered if [`fail`](https://github.com/SpinGo/op-rabbit/blob/a5e534a8d3e9b1a89544501acd334b983ecdb5c4/core/src/main/scala/com/spingo/op_rabbit/Directives.scala#L156)
is called or if a failed future is passed to `ack`. For streams, we have to do
something a little different.

To trigger the specified recovery strategy when using `op-rabbit-akka-stream`
and its `acked` components, an exception should be thrown within the `acked`
part of the graph. However, the default exception-handling behavior in
`akka-stream` is stopping the graph, which in `op-rabbit`'s case would mean
stopping the consumer and preventing further messages from being processed.
To explicitly allow the graph to continue running, a ResumingDecider supervision
strategy should be declared. (To learn more about supervision strategies please
refer to the [Akka Streams docs](https://doc.akka.io/docs/akka/current/stream/stream-error.html#supervision-strategies)).

```scala
  implicit val system = ActorSystem()
  private val rabbitControl = system.actorOf(Props[RabbitControl], name = "op-rabbit")
  // We define an ActorMaterializer with a resumingDecider supervision strategy,
  // which prevents the graph from stopping when an exception is thrown.
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy(Supervision.resumingDecider: Decider)
  )
  // As a recovery strategy, let's suppose we want all nacked messages to go to
  // an existing queue called "failed-events"
  implicit private val recoveryStrategy = RecoveryStrategy.abandonedQueue(
    7.days,
    abandonQueueName = (_: String) => "failed-events"
  )

  private val src = RabbitSource(
    rabbitControl,
    channel(qos = 3),
    consume(Queue("events")),
    body(as[String])
  )

  // This may throw an exception, in which case the defined recovery strategy
  // will be triggered and our flow will continue thanks to the resumingDecider.
  private val flow = AckedFlow[String].map(_.toInt)

  private val sink = AckedSink.foreach[Int](println)
  
  src.via(flow).to(sink).run
```

### Publishing using Akka streams

(this example uses `op-rabbit-play-json` and `op-rabbit-akka-streams`)

```scala
import com.spingo.op_rabbit._
import com.spingo.op_rabbit.stream._
import com.spingo.op_rabbit.PlayJsonSupport._
implicit val workFormat = Format[Work] // setup play-json serializer

/* Each element in source will be acknowledged after publish
   confirmation is received */
AckedSource(1 to 15).
  map(Message.queue(_, queueName)).
  to(MessagePublisherSink(rabbitControl))
  .run
```

If you can see the pattern here, combining an akka-stream rabbitmq
consumer and publisher allows for guaranteed at-least-once message
delivery from head to tail; in other words, don't acknowledge the
original message from the message queue until any and all side-effect
events have been published to other queues and persisted.

### Error notification

It's important to know when your consumers fail. Out of the box,
`op-rabbit` ships with support for logging to `slf4j` (and therefore
syslog), and also `airbrake` via `op-rabbit-airbrake`. Without any
additional signal provided by you, slf4j will be used, making error
visibility a default.

You can report errors to multiple sources by combining error logging
strategies; for example, if you'd like to report to both `slf4j` and
to `airbrake`, import / set the following implicit RabbitErrorLogging
in the scope where your consumer is instantiated:

```scala
import com.spingo.op_rabbit.{Slf4jLogger, AirbrakeLogger}

implicit val rabbitErrorLogging = Slf4jLogger + AirbrakeLogger.fromConfig
```

Implementing your own error reporting strategy is simple; here's the source code for the slf4jLogger:

```scala
object Slf4jLogger extends RabbitErrorLogging {
  def apply(
    name: String,
    message: String,
    exception: Throwable,
    consumerTag: String,
    envelope: Envelope,
    properties: BasicProperties,
    body: Array[Byte]): Unit = {

    val logger = LoggerFactory.getLogger(name)
    logger.error(s"${message}. Body=${bodyAsString(body, properties)}. Envelope=${envelope}", exception)
  }
}
```

## Notes

### Shapeless dependency

Note, Op-Rabbit depends on
[shapeless](https://github.com/milessabin/shapeless) `2.3.0`, and there is presently no published version of `spray-routing-shapeless2` which works with shapeless `2.3.0`. Consider migrating to `akka-http`, or if you must stay on spray, use [op-rabbit 1.2.x](https://github.com/SpinGo/op-rabbit/tree/v1.2.x), instead.

## Credits

Op-Rabbit was created by [Tim Harper](http://timcharper.com)

This library builds upon the excellent
[Akka RabbitMQ client](https://github.com/thenewmotion/akka-rabbitmq)
by Yaroslav Klymko.
