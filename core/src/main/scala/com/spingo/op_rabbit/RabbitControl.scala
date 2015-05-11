package com.spingo.op_rabbit

import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.pattern.{ask,pipe}
import akka.util.Timeout
import com.rabbitmq.client.AMQP.BasicProperties
import com.thenewmotion.akka.rabbitmq.{ RichConnectionActor, Channel, ConnectionFactory, ConnectionActor, CreateChannel, ChannelActor, ChannelCreated, ChannelMessage }
import com.typesafe.config.ConfigFactory
import java.net.URLEncoder
import java.nio.charset.Charset
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

trait MessageForPublication extends (Channel => Unit) {
  val dropIfNoChannel: Boolean
  def apply(c: Channel): Unit
}

object MessageForPublication {
  def unapply(m: MessageForPublication): Option[Boolean] = {
    Some(m.dropIfNoChannel)
  }
}

trait MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit
}

case class TopicPublisher(routingKey: String, exchange: String = RabbitControl topicExchangeName) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish(exchange, routingKey, properties, data)
}
case class QueuePublisher(queue: String) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish("", queue, properties, data)
}

class MarshalledMessage(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties,
  val dropIfNoChannel: Boolean = false) extends MessageForPublication {

  def apply(c: Channel): Unit =
    publisher(c, data, properties)
}

object MarshalledMessage {
  def apply[T](publisher: MessagePublisher, message: T, dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]) = {
    val (bytes, properties) = marshaller.marshallWithProperties(message)
    new MarshalledMessage(publisher, bytes, properties.build, dropIfNoChannel)
  }
}


object StatusCheckMessage {
  case class CheckException(msg: String) extends Exception(msg)
}
// This is used for checking the status of rabbitMq
class StatusCheckMessage(timeout: Duration = 5 seconds)(implicit actorSystem: ActorSystem) extends MessageForPublication {
  val dropIfNoChannel = true
  private val isOpenPromise = Promise[Unit]
  val okay = isOpenPromise.future

  private def withTimeout[T](what:String, duration: FiniteDuration)(f: => Future[T]): Future[T] = {
    import actorSystem.dispatcher
    val timer = akka.pattern.after(duration, using = actorSystem.scheduler) {
      Future.failed(new scala.concurrent.TimeoutException(s"Response not received from ${what} after ${duration}."))
    }
    Future.firstCompletedOf(timer :: f :: Nil)
  }

  def apply(c: com.rabbitmq.client.Channel): Unit = {
    isOpenPromise.tryComplete(Try {
      assert(c.isOpen(), new StatusCheckMessage.CheckException("RabbitMQ outbound channel is not open"))
    })
  }
}

object TopicMessage {
  def apply[T](message: T, routingKey: String, exchange: String = RabbitControl.topicExchangeName, dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]): MessageForPublication =
    MarshalledMessage(TopicPublisher(routingKey, exchange), message, dropIfNoChannel)
}

object QueueMessage {
  def apply[T](
    message: T,
    queue: String,
    dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]): MessageForPublication =
    MarshalledMessage(QueuePublisher(queue), message, dropIfNoChannel)
}

case object GetConnectionActor

object RabbitControl {
  lazy val topicExchangeName = ConfigFactory.load.getString("rabbitmq.topic-exchange-name")

  sealed trait SubscriptionCommand
  case object Pause extends SubscriptionCommand
  case object Run extends SubscriptionCommand
  val CONNECTION_ACTOR_NAME = "connection"
}


// pretty stupid, but the connectionFactory is pretty crappy and offers no way to specify multiple hosts; instead, you must call newConnection with the address array.
class ClusterConnectionFactory extends ConnectionFactory() {
  import com.rabbitmq.client.{Address, Connection}
  var hosts = Array.empty[String]
  def setHosts(newHosts: Array[String]): Unit = { hosts = newHosts }

  override def getHost = {
    if (hosts.nonEmpty)
      s"{${hosts.mkString(",")}}"
    else
      super.getHost
  }

  override def newConnection(): Connection = {
    if (hosts.nonEmpty)
      this.newConnection(hosts.map(host => new Address(host, getPort)))
    else
      super.newConnection()
  }
}

class RabbitControl extends Actor with ActorLogging with Stash {
  import RabbitControl._
  val config = ConfigFactory.load
  protected val rabbitMQ = config.getConfig("rabbitmq")

  private val rabbitConnectionFactory = new ClusterConnectionFactory()

  val hosts = Try { rabbitMQ.getStringList("hosts").toArray(new Array[String](0)) } getOrElse { Array(rabbitMQ.getString("host")) }
  rabbitConnectionFactory.setConnectionTimeout(rabbitMQ.getDuration("timeout", java.util.concurrent.TimeUnit.MILLISECONDS).toInt)
  rabbitConnectionFactory.setHosts(hosts)
  rabbitConnectionFactory.setUsername(rabbitMQ.getString("username"))
  rabbitConnectionFactory.setPassword(rabbitMQ.getString("password"))
  rabbitConnectionFactory.setPort(rabbitMQ.getInt("port"))

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1) {
    case _: Exception => Resume
  }

  private var subscriptions = List.empty[ActorRef]

  implicit val timeout = Timeout(5 seconds)
  implicit val ec = context.dispatcher

  val connection = context.actorOf(
    ConnectionActor.props(rabbitConnectionFactory),
    name = CONNECTION_ACTOR_NAME)
  override def preStart =
    connection ! CreateChannel(ChannelActor.props(), Some("publisher"))

  override def postStop: Unit = {
    // Don't restart the child actors!!!
  }

  def receive = {
    case ChannelCreated(ref) =>
      context.become(withChannel(ref))
      unstashAll()

    case _ => stash()
  }

  def withChannel(publishChannel: ActorRef): Receive = {
    case m @ MessageForPublication(dropIfNoChannel) =>
      publishChannel ! ChannelMessage(m.apply(_), dropIfNoChannel = dropIfNoChannel)


    case GetConnectionActor =>
      sender ! connection

    case Terminated(ref) if subscriptions.exists(_.path == ref.path) =>
      // TODO - move this logic to a subscription guardian actor? This is doing too much...
      // We need this guardian to have a supervisorStrategy which resumes child actors on failure !!! This way, we won't build up infinite number of promises
      subscriptions = subscriptions.filterNot(_.path == ref.path)

    case q: Subscription[_] =>
      val subscriptionActorRef = context.actorOf(SubscriptionActor.props(q, connection), name = s"subscription-${java.net.URLEncoder.encode(q.consumer.name)}")
      context watch subscriptionActorRef
      subscriptionActorRef ! Run
      subscriptions = subscriptionActorRef :: subscriptions

    case c: SubscriptionCommand =>
      val futures = subscriptions map (_ ? c)
      (Future.sequence(futures) map { a => Unit }) pipeTo sender
  }
}
