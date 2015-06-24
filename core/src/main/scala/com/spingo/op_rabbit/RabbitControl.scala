package com.spingo.op_rabbit

import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.pattern.{ask,pipe}
import akka.util.Timeout
import com.thenewmotion.akka.rabbitmq.{ RichConnectionActor, Channel, ConnectionFactory, ConnectionActor, CreateChannel, ChannelActor, ChannelCreated, ChannelMessage }
import com.typesafe.config.ConfigFactory
import java.net.URLEncoder
import scala.concurrent.duration._
import scala.concurrent.Future
import com.spingo.op_rabbit.subscription.Subscription

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

  val CONFIRMED_PUBLISHER_NAME = "confirmed-publisher"
  /**
    If op-rabbit doesn't do what you need it to, you can ask for the
    akka-rabbitmq ConnectionActor ActorRef by querying the
    [[RabbitControl]] actor with this.
    */
  case object GetConnectionActor
  case object GetConnection
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
  def this() = this(ConnectionParams.fromConfig())
  import RabbitControl._

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = -1) {
    case _: Exception => Resume
  }

  private var subscriptions = List.empty[ActorRef]

  implicit val timeout = Timeout(5 seconds)
  implicit val ec = context.dispatcher

  private val connectionFactory = new ClusterConnectionFactory
  connectionParams.applyTo(connectionFactory)

  val connectionActor = context.actorOf(
    ConnectionActor.props(
      connectionFactory,
      setupConnection = { (connection, _) => self ! connection }
    ),
    name = CONNECTION_ACTOR_NAME)

  val confirmedPublisher = context.actorOf(
    Props(new ConfirmedPublisherActor(connectionActor)),
    name = CONFIRMED_PUBLISHER_NAME)

  override def preStart =
    connectionActor ! CreateChannel(ChannelActor.props(), Some("publisher"))

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
    case m: ConfirmedMessage =>
      confirmedPublisher ! m

    case m: MessageForPublicationLike =>
      publishChannel ! ChannelMessage(m.apply(_), dropIfNoChannel = m.dropIfNoChannel)

    case GetConnectionActor =>
      sender ! connectionActor

    case Terminated(ref) if subscriptions.exists(_.path == ref.path) =>
      // TODO - move this logic to a subscription guardian actor? This is doing too much...
      // We need this guardian to have a supervisorStrategy which resumes child actors on failure !!! This way, we won't build up infinite number of promises
      subscriptions = subscriptions.filterNot(_.path == ref.path)

    case q: Subscription =>
      val subscriptionActorRef = context.actorOf(subscription.SubscriptionActor.props(q, connectionActor), name = s"subscription-${java.net.URLEncoder.encode(q.binding.queueName)}")
      context watch subscriptionActorRef
      // TODO - we need this actor to know the currect subscription state
      subscriptionActorRef ! Run
      subscriptions = subscriptionActorRef :: subscriptions

    case c: SubscriptionCommand =>
      val futures = subscriptions map (_ ? c)
      (Future.sequence(futures) map { a => Unit }) pipeTo sender
  }
}
