package demo

import akka.actor.{ ActorSystem, Props }
import com.spingo.op_rabbit.{ Directives, Message, PlayJsonSupport, Publisher, Queue, RabbitControl, RecoveryStrategy, Subscription }
import scala.concurrent.ExecutionContext
import play.api.libs.json._

case class Data(id: Int)

object Main extends App {
  import PlayJsonSupport._
  implicit val actorSystem = ActorSystem("demo")
  implicit val dataFormat = Json.format[Data]

  val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
  implicit val recoveryStrategy = RecoveryStrategy.nack(false)
  import ExecutionContext.Implicits.global

  val demoQueue = Queue("demo", durable = false, autoDelete = true)
  val subscription = Subscription.run(rabbitControl) {
    import Directives._
    channel(qos=3) {
      consume(demoQueue) {
        body(as[Data]) { data =>
          println(s"received ${data}")
          ack
        }
      }
    }
  }

  val publisher = Publisher.queue(demoQueue)
  (1 to Int.MaxValue) foreach { n =>
    rabbitControl ! Message(Data(n), publisher)
    Thread.sleep(1000)
  }

  println("demo")
}
