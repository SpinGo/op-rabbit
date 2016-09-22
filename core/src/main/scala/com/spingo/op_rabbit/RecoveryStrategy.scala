package com.spingo.op_rabbit

import com.rabbitmq.client.Channel
import scala.concurrent.duration._
import com.spingo.op_rabbit.properties.PimpedBasicProperties
import Directives.{property, extract, ack, exchange => dExchange, routingKey}
import com.spingo.op_rabbit.Queue.ModeledArgs.{`x-message-ttl`, `x-expires`}

/**
  * Instructs RabbitMQ what to do in the event of a consumer failure, failure being defined as the handler throwing an
  * exception, the `fail` directive being used, or `ack(Future.failed(ex))` / `ack(Failure(ex))`.
  *
  * By contract, RecoveryStrategy accept a queueName, channel, and exception, and return a [[Handler]] whose status
  * dictates the fate of the recovered message. A RecoveryStratregy should not do any asynchronous work involving the
  * provided channel
  */
trait RecoveryStrategy extends ((String, Channel, Throwable) => Handler)
private [op_rabbit] class RecoveryStrategyWrap(fn: (String, Channel, Throwable) => Handler) extends RecoveryStrategy {
  def apply(queueName: String, ch: Channel, ex: Throwable): Handler = fn(queueName, ch, ex)
}

object RecoveryStrategy {

  def apply(fn: (String, Channel, Throwable) => Handler): RecoveryStrategy = {
    new RecoveryStrategyWrap(fn)
  }

  private [op_rabbit] def formatException(ex: Throwable): String = {
    val b = new java.io.StringWriter
    ex.printStackTrace(new java.io.PrintWriter(b))
    b.toString
  }

  /** The message header used to track retry attempts. */
  val `x-retry` = properties.TypedHeader[Int]("x-retry")

  /** The original routing key before redeliver attempts */
  val `x-original-routing-key` = properties.TypedHeader[String]("x-original-routing-key")

  /** The original exchange used before redeliver attempts */
  val `x-original-exchange` = properties.TypedHeader[String]("x-original-exchange")

  /** directive which extracts the x-original-routing-key, if present. Falls back to envelope routing key. */
  val originalRoutingKey = (property(`x-original-routing-key`) | routingKey)

  /** directive which extracts the x-original-exchange, if present. Falls back to envelope exchange. */
  val originalExchange = (property(`x-original-exchange`) | dExchange)

  /** Header at which the formatted exception is stored. */
  val `x-exception` = properties.TypedHeader[String]("x-exception")

  /** Places copy of message into a queue with "op-rabbit.abandoned." prepended (configurable); after ttl (default of 1
    * day), these messages are dropped. Exception is stored in `x-exception` header; original routing information stored
    * in `x-original-routing-key` and `x-original-exchange`.
    *
    * @param defaultTTL - How long abandoned messages should stay in the queue.
    * @param abandonTo - Pure function which dictates, given some input queue, the name of the abandon queue.
    */
  def abandonedQueue(
    defaultTTL: FiniteDuration = 1.day,
    abandonQueueName: (String) => String = { q => s"op-rabbit.abandoned.$q" },
    abandonQueueProperties: List[properties.Header] = Nil,
    exchange: Exchange[Exchange.Direct.type] = Exchange.default
  ) = RecoveryStrategy {
    (queueName, channel, exception) =>

    (extract(identity) & originalRoutingKey & originalExchange) { (delivery, rk, x) =>
      val binding = Binding.direct(
        Queue.passive(
          Queue(
            abandonQueueName(queueName),
            durable = true,
            arguments = List[properties.Header](
              `x-message-ttl`(defaultTTL),
              `x-expires`(defaultTTL * 2)) ++
              abandonQueueProperties)),
        exchange)
      binding.declare(channel)
      channel.basicPublish("",
        binding.queueName,
        delivery.properties ++ Seq(
          `x-exception`(formatException(exception)),
          `x-original-routing-key`(rk),
          `x-original-exchange`(x)),
        delivery.body)
      ack
    }
  }


  /**
   * Recovery strategy which redelivers messages a limited number of times. '''Violates message ordering guarantees.'''
   *
   * For a definition of what constitutes failure, see [[RecoveryStrategy]].
   *
   * In the event of a failure, the following happens:
   *
   *   - The message header [[x-retry `x-retry`]] is read to determine if there are retries remaining.
   *
   *   - If retries are exhausted, then fallback to provided `onAbandon` handler.
   *
   *   - Otherwise, publish a new copy of the message to the configurable, passively created `retryQueue`. It is of
   *     '''vital importance''' that you read the queue caveats related noted below.
   *
   *     - [[x-retry `x-retry`]] is incremented.
   *
   *     - The passive-queue definition is attempted at every retry. This leads to more IO but prevents the queue from
   *       expiring and basicPublish being none-the-wiser.
   *
   *     - The message copy is published using `channel.basicPublish`. Sometime after, using the same channel, the
   *       original message is acknowledged, using the same channel. This means two things:
   *
   *         1) The publish could succeed and the acknowledgement could fail, leading to a duplication of the
   *            message. An IO exception or network event at the right time could cause this.
   *
   *         2) In certain clustering configurations, it might be possible for the basicPublish to fail, but the
   *            acknowledgement to succeed. The failure case would require that the the message queue and the redelivery
   *            queue are on different cluster nodes, and that one can be reached but the other is not. Plan as
   *            needed. An implementation using transactions (slow) / asynchronous publisher confirms (faster) may be
   *            warranted.
   *
   *     - The message copy is re-inserted into the queue using the default (direct) exchange. This means that the
   *       second time around, if a direct-exchange wasn't used to initially route the message into the queue, then the
   *       routingKey will be modified. See the message header [[x-original-routing-key `x-original-routing-key`]] for
   *       the original routing key.
   *
   * == retryQueue passive creations CAVEATS!!! ==
   *
   * The retry queue is created passively. This allows the retryQueue to be used without error in the case of a
   * configuration change (in cases where a crash would be worse), but means that configuration changes done after the
   * initial creation of the queue will not propagate. As a result, if you want to guarantee that changes made to your
   * redelivery strategy propagate that you modify `retryVia` function such that it will create a new redelivery
   * queue. The old, unused one will expire, after an idle period of `redeliveryDelay * 3`.
   *
   * Also, it is vitally important that for every input queue-name of the `retryVia`, a unique value is returned. In
   * other words, '''DO NOT TRY AND CONSOLIDATE REDELIVERY QUEUES'''.
   *
   * @param redeliveryDelay The period after which the message should be re-inserted into the original queue.
   * @param retryCount The number of times to retry. A value of 3 will result in 4 attempts, including the initial.
   * @param onAbandon The strategy to be used once the retryCount is exhausted. Defaults to `nack(false)`.
   * @param retryQueue A function which, given some input queue name, returns the queue name to be used for retries. Must be a pure function. Must return a unique value for each input.
   */
  def limitedRedeliver(
    redeliverDelay: FiniteDuration = 10.seconds,
    retryCount: Int = 3,
    onAbandon: RecoveryStrategy = nack(false),
    retryQueueName : (String) => String = { q => s"op-rabbit.retry.$q" },
    retryQueueProperties: List[properties.Header] = Nil,
    exchange: Exchange[Exchange.Direct.type] = Exchange.default
  ) = new RecoveryStrategy {
    import Queue.ModeledArgs._

    def genRetryBinding(queueName: String) =
      Binding.direct(
        Queue.passive(
          Queue(
            retryQueueName(queueName),
            durable = true,
            arguments = List[properties.Header](
              `x-expires`(redeliverDelay * 3),
              `x-message-ttl`(redeliverDelay),
              `x-dead-letter-exchange`(""), // default exchange
              `x-dead-letter-routing-key`(queueName)) ++ retryQueueProperties)),
        exchange)

    private val getRetryCount = (property(`x-retry`) | Directives.provide(0))
    def apply(queueName: String, channel: Channel, ex: Throwable): Handler = {
      getRetryCount {
        case (thisRetryCount) if (thisRetryCount < retryCount) =>
          (extract(identity) & originalRoutingKey & originalExchange) {
            (delivery, rk, x) =>

            /* It is important that we create a new instance of the passive retry queue each time it is used; otherwise,
             * we risk that the queue could disappear due to `x-expires`, and messages dead-letter. */
            val binding = genRetryBinding(queueName)
            binding.declare(channel)

            channel.basicPublish("",
              binding.queueName,
              delivery.properties ++ Seq(
                `x-retry`(thisRetryCount + 1),
                `x-original-routing-key`(rk),
                `x-original-exchange`(x),
                `x-exception`(formatException(ex))),
              delivery.body)
            ack
          }
        case _ =>
          onAbandon(queueName, channel, ex)
      }
    }
  }

  /** Nack (reject) the message.
    *
    * @param requeue Whether the message should be requeued for delivery. Note: true could cause an infinite loop */
  def nack(requeue: Boolean = false): RecoveryStrategy = new RecoveryStrategy {
    def apply(queueName: String, channel: Channel, ex: Throwable): Handler =
      Directives.nack(requeue)
  }

  /** No recovery strategy; cause the consumer to shutdown with an exception.
    */
  val none: RecoveryStrategy = new RecoveryStrategy {
    def apply(queueName: String, channel: Channel, ex: Throwable): Handler =
      Directives.ack(scala.util.Failure(ex))
  }
}
