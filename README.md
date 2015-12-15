# Op-Rabbit

##### An opinionated RabbitMQ library for Scala and Akka.

# Documentation

Browse the latest [API Docs](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/current/index.html) online.

For announcements, discussions, etc., join the discussion:

- [Op-rabbit discussion forum](https://groups.google.com/forum/#!forum/op-rabbit)

Issues go here:

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

Add the SpinGo OSS repository and include the dependencies of your choosing:

```scala
resolvers ++= Seq(
  "SpinGo OSS" at "http://spingo-oss.s3.amazonaws.com/repositories/releases"
)

val opRabbitVersion = "1.1.2"

libraryDependencies ++= Seq(
  "com.spingo" %% "op-rabbit-core"        % opRabbitVersion,
  "com.spingo" %% "op-rabbit-play-json"   % opRabbitVersion,
  "com.spingo" %% "op-rabbit-json4s"      % opRabbitVersion,
  "com.spingo" %% "op-rabbit-airbrake"    % opRabbitVersion,
  "com.spingo" %% "op-rabbit-akka-stream" % opRabbitVersion
)
```

### Version Compatibility Matrix:

1.1.x

| module                       | dependsOn                | version  |
| ---------------------------- | ------------------------ | -------- |
| op-rabbit-core               | akka                     | 2.4.x    |
|                              | akka-rabbitmq            | 2.x      |
|                              | shapeless                | 2.2.x    |
|                              | type-safe config         | >= 1.3.0 |
| op-rabbit-play-json          | play-json                | 2.4.x    |
| op-rabbit-json4s             | json4s                   | 3.2.x    |
| op-rabbit-airbrake           | airbrake                 | 2.2.x    |
| op-rabbit-akka-steam         | acked-stream             | 1.0      |
|                              | akka-stream-experimental | 1.0      |
| op-rabbit-akka-steam-2.0-M1  | acked-stream             | 2.0-M1-2 |
|                              | akka-stream-experimental | 2.0-M1   |

1.0.x

| module                       | dependsOn                | version  |
| ---------------------------- | ------------------------ | -------- |
| op-rabbit-core               | akka                     | 2.2.x    |
|                              | akka-rabbitmq            | 1.3.x    |
|                              | shapeless                | 2.2.x    |
|                              | type-safe config         | >= 1.3.0 |
| op-rabbit-play-json          | play-json                | 2.4.x    |
| op-rabbit-json4s             | json4s                   | 3.2.x    |
| op-rabbit-airbrake           | airbrake                 | 2.2.x    |
| op-rabbit-akka-steam         | acked-stream             | 1.0      |
|                              | akka-stream-experimental | 1.0      |

## A high-level overview of the available components:

- `op-rabbit-core` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html)
    - Implements basic patterns for serialization and message
      processing.
- `op-rabbit-play-json` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/play-json/current/index.html)
    - Easily use
      [Play Json](https://www.playframework.com/documentation/2.4.x/ScalaJson)
      formats to publish or consume messages; automatically sets
      RabbitMQ message headers to indicate content type.
- `op-rabbit-json4s` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/json4s/current/index.html)
    - Easily use [Json4s](http://json4s.org) to serialization
      messages; automatically sets RabbitMQ message headers to
      indicate content type.
- `op-rabbit-airbrake` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/airbrake/current/index.html)
    - Report consumer exceptions to airbrake, using the
      [Airbrake](https://github.com/airbrake/airbrake-java) Java
      library.
- `op-rabbit-akka-stream` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/akka-stream/current/index.html)
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
  connection {
    virtual-host = "/"
    hosts = ["127.0.0.1"]
    username = "guest"
    password = "guest"
    port = 5672
    timeout = 3s
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
[AckedSink](https://github.com/timcharper/acked-stream/blob/master/src/main/scala/com/timcharper/acked/AckedSink.scala)
(such as
[`MessagePublisherSink`](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/akka-stream/current/index.html#com.spingo.op_rabbit.stream.MessagePublisherSink$)). You
can convert an acked stream into a normal stream by calling
`AckedStream.acked`; once messages flow passed the `acked` component,
they are considered acknowledged, and acknowledgement tracking is no
longer a concern (and thus, you are free to use the akka-stream
library in it's entirety).

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
[shapeless](https://github.com/milessabin/shapeless) `2.2.3`; if you
are using `spray`, then you'll need to use the
[version built for shapeless `2.1.0`](http://repo.spray.io/io/spray/spray-routing-shapeless2_2.11/1.3.3/);
shapeless `2.2.3` is
[noted to be binary compatible with `2.1.x` in most cases](https://github.com/milessabin/shapeless/blob/e78c95926550a1f9a6ca82fad07548ddaedd4901/notes/2.2.2.markdown).

## Credits

Op-Rabbit was created by [Tim Harper](http://timcharper.com)

This library builds upon the excellent
[Akka RabbitMQ client](https://github.com/thenewmotion/akka-rabbitmq)
by Yaroslav Klymko.
