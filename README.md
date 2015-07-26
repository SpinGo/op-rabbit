# Op-Rabbit

##### An opinionated RabbitMQ library for Scala and Akka.

See the latest API Docs:

- [op-rabbit-core](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html) (Start here)
- [op-rabbit-json4s](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/json4s/current/index.html)
- [op-rabbit-play-json](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/play-json/current/index.html)
- [op-rabbit-airbrake](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/airbrake/current/index.html)
- [op-rabbit-akka-stream](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/akka-stream/current/index.html)

For announcements, discussions, etc., join the discussion:

- [https://groups.google.com/forum/#!forum/op-rabbit](Op-rabbit discussion forum)

Issues go here:

- https://github.com/SpinGo/op-rabbit/issues

# Intro

Op-Rabbit is a high-level, opinionated, composable, fault-tolerant library for interacting with RabbitMQ; the following is a high-level feature list:

- Recovery:
    - Consumers automatically reconnect and subscribe if the connection is lost
    - Messages published will wait for a connection to be available
- Integration
    - Connection settings pulled from Typesafe config library
    - Asyncronous, concurrent consumption using Scala native Futures or the new Akka Streams project.
    - Common pattern for serialization allows easy integration with serialization libraries such play-json or json4s
    - Common pattern for exception handling to publish errors to Airbrake, Syslog, or all of the above
- Modular
    - Composition favored over inheritance enabling flexible and high code reuse.
- Modeled
    - Queue binding, exchange binding modeled with case classes
    - Publishing mechansims also modeled
- Reliability
    - Builds on the excellent [Akka RabbitMQ client](https://github.com/thenewmotion/akka-rabbitmq) library for easy recovery.
    - Built-in consumer error recovery strategy in which messages are re-delivered to the message queue and retried (not implemented for akka-streams integration as retry mechanism affects message order)
    - With a single message, pause all consumers if service health check fails (IE: database unavailable); easily resume the same.
- Graceful shutdown
    - Consumers and streams can immediately unsubscribe, but stay alive long enough to wait for any messages to finish being processed.
- Program at multiple levels of abstraction
    - If op-rabbit doesn't do what you need it to, you can either extend op-rabbit or interact directly with `akka-rabbitmq` [Akka RabbitMQ client](https://github.com/thenewmotion/akka-rabbitmq).
- Tested
    - Extensive integration tests

## Installation

Add the SpinGo OSS repository and include the dependencies of your choosing:

```scala
resolvers ++= Seq(
  "SpinGo OSS" at "http://spingo-oss.s3.amazonaws.com/repositories/releases"
)

val opRabbitVersion = "1.0.0-M11"

libraryDependencies ++= Seq(
  "com.spingo" %% "op-rabbit-core"        % opRabbitVersion,
  "com.spingo" %% "op-rabbit-play-json"   % opRabbitVersion,
  "com.spingo" %% "op-rabbit-json4s"      % opRabbitVersion,
  "com.spingo" %% "op-rabbit-airbrake"    % opRabbitVersion,
  "com.spingo" %% "op-rabbit-akka-stream" % opRabbitVersion
)
```

A high-level overview of the available components:

- `op-rabbit-core` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html)
    - Implements basic patterns for serialization and message processing.
- `op-rabbit-play-json` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/play-json/current/index.html)
    - Easily use [Play Json](https://www.playframework.com/documentation/2.4.x/ScalaJson) formats to publish or consume messages; automatically sets RabbitMQ message headers to indicate content type.
- `op-rabbit-json4s` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/json4s/current/index.html)
    - Easily use [Json4s](http://json4s.org) to serialization messages; automatically sets RabbitMQ message headers to indicate content type.
- `op-rabbit-airbrake` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/airbrake/current/index.html)
    - Report consumer exceptions to airbrake.
- `op-rabbit-akka-stream` [API](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/akka-stream/current/index.html)
    - Process or publish messages using akka-stream.

## Usage

Set up RabbitMQ connection information in `application.conf`:

```conf
rabbitmq {
  topic-exchange-name = "op-rabbit-testeroni"
  virtual-host = "/"
  hosts = ["127.0.0.1"]
  username = "guest"
  password = "guest"
  port = 5672
  timeout = 3s
}
```

Note that hosts is an array; Connection attempts will be made to hosts in that order, with a default timeout of `3s`. This way you can specify addresses of your rabbitMQ cluster, and if one of the instances goes down, your application will automatically reconnect to another member of the cluster.

`topic-exchange-name` is the default topic exchange to use; this can be overriden by passing `exchange = "my-topic"` to TopicBinding or TopicMessage.


Boot up the RabbitMQ control actor:

```scala
implicit val actorSystem = ActorSystem("such-system")
val rabbitMq = actorSystem.actorOf(Props[RabbitControl])
```

### Set up a consumer: (Topic subscription)

(this example uses `op-rabbit-play-json`)

```scala
import com.spingo.op_rabbit.PlayJsonSupport._
import com.spingo.op_rabbit._
import com.spingo.op_rabbit.consumer._
import com.spingo.op_rabbit.subscription.Directives._

import scala.concurrent.ExecutionContext.Implicits.global
implicit val personFormat = Json.format[Person] // setup play-json serializer

val subscription = new Subscription {
  // A qos of 3 will cause up to 3 concurrent messages to be processed at any given time.
  def config = channel(qos = 3) {
    consume(topic("such-message-queue", List("some-topic.#"))) {
      body(as[Person]) { person =>
        // do work; this body is executed in a separate thread, as provided by the implicit execution context
        ack()
      }
    }
  }
}

rabbitMq ! subscription
```

Note, if your call generates an additional future, you can pass it to ack, and message will be acked based off the Future success, and nacked if the Future fails:

```scala
      body(as[Person]) { person =>
        // do work; this body is executed in a separate thread, as provided by the implicit execution context
        val result: Future[Unit] = myApi.methodCall(person)
        ack(result)
      }

```

#### Accessing additional headers

If there are other headers you'd like to access, you can extract multiple using nested functions, or combine multiple directives to a single one. IE:

```scala
import com.spingo.op_rabbit.properties._

// Nested directives
// ...
      body(as[Person]) { person =>
        optionalProperty(ReplyTo) { replyTo =>
          // do work
          ack()
        }
      }
// ...

// Compound directive
// ...
      (body(as[Person]) & optionalProperty(ReplyTo)) { (person, replyTo) =>
        // do work
        ack()
      }
// ...
```

Please see the documentation on [Directives](http://spingo-oss.s3.amazonaws.com/docs/op-rabbit/core/current/index.html#com.spingo.op_rabbit.consumer.Directives) for more details.

#### Shutting down a consumer

The following methods are available on subscription which will allow control over the subscription.

```scala
// stop receiving new messages from RabbitMQ immediately; shut down consumer and channel as soon as pending messages are completed. A grace period of 30 seconds is given, after which the subscription forcefully shuts down.
subscription.close(30 seconds)

// Shut things down without a grace period
subscription.abort()

// Future[Unit] which completes once the provided binding has been applied (IE: queue has been created and topic bindings configured). Useful if you need to assert you don't send a message before a message queue is created in which to place it.
subscription.initialized

// Future[Unit] which completes when the subscription is closed.
subscription.closed

// Future[Unit] which completes when the subscription begins closing.
subscription.closing
```

### Publish a message:

```scala
rabbitMq ! TopicMessage(Person(name = "Mike How", age = 33), routingKey = "some-topic.very-interest")

rabbitMq ! QueueMessage(Person(name = "Ivanah Tinkle", age = 25), queue = "such-message-queue")
```

By default:

- Messages will be queued up until a connection is available
- Messages are monitored via publisherConfirms; if a connection is lost before RabbitMQ confirms receipt of the message, then the message is published again. This means that the message may be delivered twice, the default opinion being that `at-least-once` is better than `at-most-once`. You can use `UnconfirmedMessage` if you'd like `at-most-once` delivery instead.
- If you would like to be notified of confirmation, use the ask pattern:

      ```scala
        val received = (rabbitMq ? QueueMessage(Person(name = "Ivanah Tinkle", age = 25), queue = "such-message-queue")).mapTo[Boolean]
      ```

### Consuming using Akka streams

(this example uses `op-rabbit-play-json` and `op-rabbit-akka-streams`)

```scala
import com.spingo.op_rabbit._
import com.spingo.op_rabbit.subscription._
import com.spingo.op_rabbit.subscription.Directives._
import com.spingo.op_rabbit.PlayJsonSupport._
implicit val workFormat = Json.format[Work] // setup play-json serializer

RabbitSource(
  rabbitMq,
  channel(qos = 3),
  consume(queue("such-queue", durable = true, exclusive = false, autoDelete = false)),
  body(as[Work])). // marshalling is automatically hooked up using implicits
  runForeach { work =>
    doWork(work)
  } // after each successful iteration the message is acknowledged.
  .run
```

Note: `RabbitSource` yields an AckedSource, which can be combined with an AckedSink (such as `ConfirmedPublisherSink`). You can convert an acked stream into a normal stream by calling `AckedStream.acked`; once messages flow passed the `acked` component, they are considered acknowledged, and acknowledgement tracking is no longer a concern (and thus, you are free to use the akka-stream library in it's entirety).

### Publishing using Akka streams

(this example uses `op-rabbit-play-json` and `op-rabbit-akka-streams`)

```scala
import com.spingo.op_rabbit._
import com.spingo.op_rabbit.stream._
import com.spingo.op_rabbit.PlayJsonSupport._
implicit val workFormat = Format[Work] // setup play-json serializer

val sink = ConfirmedPublisherSink[Work](
  "my-sink-name",
  rabbitMq,
  ConfirmedMessage.factory(QueuePublisher(queueName())))

AckedSource(1 to 15). // Each element in source will be acknowledged after publish confirmation is received
  to(sink)
  .run
```

If you can see the pattern here, combining an akka-stream rabbitmq consumer and publisher allows for guaranteed at-least-once message delivery from head to tail; in other words, don't acknowledge the original message from the message queue until any and all side-effect events have been published to other queues and persisted.

### Error notification

It's important to know when your consumers fail. Out of the box, `op-rabbit` ships with support for logging to `logback` (and therefore syslog), and also `airbrake` via `op-rabbit-airbrake`. Without any additional signal provided by you, logback will be used, making error visibility a default.

You can report errors to multiple sources by combining error logging strategies; for example, if you'd like to report to both `logback` and to `airbrake`, import / set the following implicit RabbitErrorLogging in the scope where your consumer is instantiated:

```scala
import com.spingo.op_rabbit.consumer.LogbackLogger
import com.spingo.op_rabbit.RabbitControl

implicit val rabbitErrorLogging = LogbackLogger + AirbrakeLogger.fromConfig
```

Implementing your own error reporting strategy is simple; here's the source code for the LogbackLogger:

```scala
object LogbackLogger extends RabbitErrorLogging {
  def apply(name: String, message: String, exception: Throwable, consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val logger = LoggerFactory.getLogger(name)
    logger.error(s"${message}. Body=${bodyAsString(body, properties)}. Envelope=${envelope}", exception)
  }
}
```

### Notes

#### Shapeless dependency

Note, Op-Rabbit depends on [shapeless](https://github.com/milessabin/shapeless) `2.2.3`; if you are using `spray`, then you'll need to use the [version built for shapeless `2.1.0`](http://repo.spray.io/io/spray/spray-routing-shapeless2_2.11/1.3.3/); shapeless `2.2.3` is [noted to be binary compatible with `2.1.x` in most cases](https://github.com/milessabin/shapeless/blob/e78c95926550a1f9a6ca82fad07548ddaedd4901/notes/2.2.2.markdown).

### Credits

This library builds upon the excellent [Akka RabbitMQ client](https://github.com/thenewmotion/akka-rabbitmq) by Yaroslav Klymko.
