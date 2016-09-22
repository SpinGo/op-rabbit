package com.spingo.op_rabbit

import akka.actor._
import com.spingo.op_rabbit.helpers.RabbitTestHelpers
import com.spingo.op_rabbit.properties.Header
import com.spingo.scoped_fixtures.ScopedFixtures
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.Promise
import scala.concurrent.ExecutionContext

class QueueSpec extends FunSpec with ScopedFixtures with Matchers with RabbitTestHelpers {
  val _queueName = ScopedFixture[String] { setter =>
    val name = s"test-queue-rabbit-control-${Math.random()}"
    deleteQueue(name)
    val r = setter(name)
    deleteQueue(name)
    r
  }

  describe("Queue.passive") {
    it("subscribes in such a way that the second subscription succeeds even if the queue doesn't match config") {

      import ExecutionContext.Implicits.global
      implicit val recoveryStrategy = RecoveryStrategy.none
      val consumerResult = List(Promise[String], Promise[String])

      def getSubscription(p: Promise[Unit], leQueue: Binding.QueueDefinition[Binding.Concrete]) =
        Subscription.run(rabbitControl) {
          import Directives._
          channel(qos = 1) {
            consume(leQueue) {
              body(as[String]) { _ =>
                p.success(())
                ack
              }
            }
          }
        }
      val queue = Queue(_queueName(), autoDelete = true)
      val passiveQueue = Queue.passive(Queue(_queueName(), autoDelete = false))

      // we test passiveQueue a third time to check that the caching mechanism doesn't cause an issue
      List(queue, passiveQueue, passiveQueue).foreach { q =>
        val p = Promise[Unit]
        val subscription = getSubscription(p, q)
        await(subscription.initialized)
        rabbitControl ! Message.queue("hi", q.queueName)
        await(p.future)
        subscription.close()
        await(subscription.closed)
      }
    }
  }

}
