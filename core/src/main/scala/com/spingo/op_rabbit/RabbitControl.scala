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
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
  Basic interface; send to [[RabbitControl]] actor for delivery.
  */
trait MessageForPublicationLike extends (Channel => Unit) {
  val dropIfNoChannel: Boolean
  def apply(c: Channel): Unit
}

object MessageForPublicationLike {
  def unapply(m: MessageForPublicationLike): Option[Boolean] = {
    Some(m.dropIfNoChannel)
  }
}

/**
  Common interface for publication strategies

  @see [[TopicPublisher]], [[QueuePublisher]]
  */
trait MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit
}

/**
  Publishes messages to specified topic; note that this is a strategy which receives message data and publishes it to a channel.

  @param routingKey The routing key (or topic)
  @param exchange The exchange to which the strategy will publish the message
  
  @see [[QueuePublisher]], [[MessageForPublication]]
  */
case class TopicPublisher(routingKey: String, exchange: String = RabbitControl topicExchangeName) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish(exchange, routingKey, properties, data)
}

/**
  Publishes messages directly to the specified message-queue; note that this is a strategy which receives message data and publishes it to a channel.
  
  @see [[TopicPublisher]], [[MessageForPublication]]
  */
case class QueuePublisher(queue: String) extends MessagePublisher {
  def apply(c: Channel, data: Array[Byte], properties: BasicProperties): Unit =
    c.basicPublish("", queue, properties, data)
}

/**
  Describes a messages data, along with publication strategy; send to [[RabbitControl]] actor for delivery.
  
  Use the factory method [[MessageForPublication$.apply]] to instantiate one of these using a [[RabbitMarshaller]].

  @see [[MessageForPublication$]]
  */
class MessageForPublication(
  val publisher: MessagePublisher,
  val data: Array[Byte],
  val properties: BasicProperties,
  val dropIfNoChannel: Boolean = false) extends MessageForPublicationLike {

  def apply(c: Channel): Unit =
    publisher(c, data, properties)
}


object MessageForPublication {

  /**
    Factory method for instantiating a MessageForPublication

    Note, serialization occurs in the thread calling the constructor, not the actor thread responsible for sending messages.
    
    @param publisher [[MessagePublisher]] which defines how and to where this message will be published.
    @param message The message data.
    @param dropIfNoChannel If a channel is not available at the time this message is sent, queue the message up internally and send as soon as a connection is available. Default true.
    @param marshaller The implicit [[RabbitMarshaller]] used to serialize data T to binary.
    */
  def apply[T](publisher: MessagePublisher, message: T, dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]) = {
    val (bytes, properties) = marshaller.marshallWithProperties(message)
    new MessageForPublication(publisher, bytes, properties.build, dropIfNoChannel)
  }
}


object StatusCheckMessage {
  case class CheckException(msg: String) extends Exception(msg)
}
/**
  Send this message to RabbitControl to check the status of our connection to the RabbitMQ broker.
  */
class StatusCheckMessage(timeout: Duration = 5 seconds)(implicit actorSystem: ActorSystem) extends MessageForPublicationLike {
  val dropIfNoChannel = true
  private val isOpenPromise = Promise[Unit]

  /**
    Future fails with [[StatusCheckMessage$.CheckException CheckException]] if connection is not okay
    */
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
  def apply[T](message: T, routingKey: String, exchange: String = RabbitControl.topicExchangeName, dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]): MessageForPublicationLike =
    MessageForPublication(TopicPublisher(routingKey, exchange), message, dropIfNoChannel)
}

object QueueMessage {
  def apply[T](
    message: T,
    queue: String,
    dropIfNoChannel: Boolean = false)(implicit marshaller: RabbitMarshaller[T]): MessageForPublicationLike =
    MessageForPublication(QueuePublisher(queue), message, dropIfNoChannel)
}

object RabbitControl {
  /**
    The configured default topic exchangeName
    */
  lazy val topicExchangeName = ConfigFactory.load.getString("rabbitmq.topic-exchange-name")

  /**
    Commands used to [[Pause]] and [[Run]] / Resume all consumers; These can be useful to send if application is determined unhealthy (IE: the database connection was lost, or some other important resource)
    */
  sealed trait SubscriptionCommand
  /**
    Send to [[RabbitControl]] to cause all consumers to momentarily pause consuming new messages; This can be useful to send if application is determined unhealthy (IE: the database connection was lost, or some other important resource)
    */
  case object Pause extends SubscriptionCommand
  /**
    Send to [[RabbitControl]] to cause all consumers to resume consuming new messages
    */
  case object Run extends SubscriptionCommand

  /**
    The akka-rabbitmq actor name, which runs as a child to the [[RabbitControl]] actor.
    */
  val CONNECTION_ACTOR_NAME = "connection"

  /**
    If op-rabbit doesn't do what you need it to, you can ask for the
    akka-rabbitmq ConnectionActor ActorRef by querying the
    [[RabbitControl]] actor with this.
    */
  case object GetConnectionActor
}

/**
  == Overview ==
  
  RabbitControl is the top-level actor which handles the following:

  - Pull configuration from the rabbitmq config block, and establish connection to RabbitMQ
  - Manage [[Subscription subscriptions]]
  
  == Messages received ==
  
  RabbitControl accepts the following commands / queries:

  - [[MessageForPublication]] - Publish the given message
  - [[RabbitControl$.SubscriptionCommand RabbitControl.SubscriptionCommand]] - [[RabbitControl$.Pause Pause]] / [[RabbitControl$.Run Resume]] all register subscriptions (consumers)
  - [[RabbitControl$.GetConnectionActor RabbitControl.GetConnectionActor]] - Return the akka.actor.ActorRef for the `akka-rabbitmq` ConnectionActor
  - [[Subscription]] - Activate the given subscription
  */
class RabbitControl(connectionParams: ConnectionParams) extends Actor with ActorLogging with Stash {
  def this() = this(ConnectionParams.fromConfig)
  import RabbitControl._

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1) {
    case _: Exception => Resume
  }

  private var subscriptions = List.empty[ActorRef]

  implicit val timeout = Timeout(5 seconds)
  implicit val ec = context.dispatcher

  private val connectionFactory = new ClusterConnectionFactory
  connectionParams.applyTo(connectionFactory)

  val connection = context.actorOf(
    ConnectionActor.props(connectionFactory),
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
    case m @ MessageForPublicationLike(dropIfNoChannel) =>
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
