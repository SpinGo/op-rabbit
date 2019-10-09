package com.spingo.op_rabbit.properties

import scala.language.implicitConversions
import com.rabbitmq.client.AMQP.BasicProperties.Builder
import com.rabbitmq.client.AMQP.BasicProperties
import com.spingo.op_rabbit.Rejection
import scala.concurrent.duration._

/**
  Describes a [[Header]] without a value.  Can be used in conjunction with [[Directives.property]] to extract a
  [[HeaderValue]] from messages. Also, cab be applied with a [[HeaderValue]] to create a [[Header]].
  */
case class UnboundHeader(name: String) extends (HeaderValue => Header) with PropertyExtractor[HeaderValue] with HashMapExtractor[HeaderValue] {
  override val extractorName = s"Header(${name})"
  def extract(properties: BasicProperties) =
    Option(properties.getHeaders).flatMap(extract)

  def extract(h: java.util.Map[String, Object]) =
    Option(h.get(name)) map (HeaderValue.from(_))

  def apply(value: HeaderValue) = Header(name, value)
}


/** Describes a [[TypedHeader]] without a value. Can be used in conjunction with [[Directives.property]] to extract a
  * T from a message's headers. Also, can be applied with T to created a [[TypedHeader]].
  *
  * Rather than instantiate these directly, use [[TypedHeader$]].{{apply}}.
  *
  * See [[TypedHeader]]
  */
trait UnboundTypedHeader[T] extends (T => TypedHeader[T]) with PropertyExtractor[T] with HashMapExtractor[T] {
  val name: String
  protected implicit val toHeaderValue: ToHeaderValue[T, HeaderValue]
  protected val fromHeaderValue: FromHeaderValue[T]

  override final def extractorName = s"Header($name)"

  final def extract(properties: BasicProperties): Option[T] =
    Option(properties.getHeaders).flatMap(extract)

  final def extract(m: java.util.Map[String, Object]): Option[T] =
    UnboundHeader(name).extract(m) flatMap { hv =>
      fromHeaderValue(hv) match {
        case Left(ex) => throw new Rejection.ParseExtractRejection(
          s"Header ${name} exists, but value could not be converted to ${fromHeaderValue.manifest}", ex)
        case Right(v) => Some(v)
      }
    }

  final def apply(value: T) =
    TypedHeader(name, value)

  final def untyped: UnboundHeader =
    UnboundHeader(name)
}

private [op_rabbit] case class UnboundTypedHeaderLongToFiniteDuration(name: String)
    extends UnboundTypedHeader[FiniteDuration] {
  protected val toHeaderValue = { d: FiniteDuration => HeaderValue(d.toMillis) }
  protected val fromHeaderValue = implicitly[FromHeaderValue[Long]].map(_.millis)
}

private [op_rabbit] case class UnboundTypedHeaderImpl[T](name: String)(implicit
  protected val fromHeaderValue: FromHeaderValue[T],
  protected val toHeaderValue: ToHeaderValue[T, HeaderValue])
    extends UnboundTypedHeader[T]

/**
  TypedHeader describes a RabbitMQ Message Header or Queue / Exchange / Binding argument; its type is known, and
  implicit proof is required to prove that the type can be safely converted to a supported RabbitMQ type.

  If you use custom headers, using [[TypedHeader]] is preferred over [[Header]]. For example, if you use the header
  `x-retry`, and this header contains an integer, using [[TypedHeader]] to extract and set the header will enable the
  type system enforce that fact:

  {{{
  val RetryHeader = TypedHeader[Int]("x-retry")

  Subscription {
    channel() {
      consume(queue("name")) {
        property(RetryHeader) { retries: Int =>
          // ...
          ack
        }
      }
    }
  }

  rabbitControl ! Message.queue("My body", "name", Seq(RetryHeader(5)))
  }}}
  */
class TypedHeader[T] protected (val name: String, val value: T)(implicit
  converter: ToHeaderValue[T, HeaderValue])
    extends MessageProperty {
  def insert(headers: HeaderMap): Unit =
    headers.put(name, converter(value).serializable)

  def insert(builder: Builder, headers: HeaderMap): Unit =
    insert(headers)

  def untyped: Header =
    Header(name, converter(value))
}

/**
  See [[TypedHeader]]
  */
object TypedHeader {
  /** Creates a [[TypedHeader]] bound to the provided value. See [[TypedHeader]].
    */
  def apply[T](name: String, value: T)(implicit converter: ToHeaderValue[T, HeaderValue]): TypedHeader[T] =
    new TypedHeader(name, value)

  /** Creates an [[UnboundTypedHeader]]. See [[TypedHeader]].
    */
  def apply[T](headerName: String)(implicit
    conversion: FromHeaderValue[T],
    converter: ToHeaderValue[T, HeaderValue]): UnboundTypedHeader[T] =
    UnboundTypedHeaderImpl(headerName)

  implicit def typedHeaderToHeader[T](h: TypedHeader[T]): Header = h.untyped
}

/**
  Header describes a RabbitMQ Message Header or Queue / Exchange / Binding argument; its type is unknown, but the
  subspace of valid RabbitMQ types are modeled with [[HeaderValue]], preventing accidental usage of an unsupported
  type by RabbitMQ.

  If you use custom headers, using [[TypedHeader]] is preferred over [[Header]].

  You can instantiate an [[UnboundHeader]] for use in both reading and writing the header:

  {{{
  val RetryHeader = Header("x-retry")

  Subscription {
    channel() {
      consume(queue("name")) {
        property(RetryHeader.as[Int]) { retries =>
          // ...
          ack
        }
      }
    }
  }

  rabbitControl ! Message.queue("My body", "name", Seq(RetryHeader(5)))
  }}}
  */
class Header protected (val name: String, val value: HeaderValue) extends MessageProperty {
  def insert(headers: HeaderMap): Unit =
    headers.put(name, value.serializable)

  def insert(builder: Builder, headers: HeaderMap): Unit =
    insert(headers)
}

/**
  See [[Header]]
  */
object Header {
  /** Creates a [[Header]] bound to the provided value. See [[Header]].
    */
  def apply(name: String, value: HeaderValue): Header = {
    if (value == null)
      new Header(name, HeaderValue.NullHeaderValue)
    else
      new Header(name, value)
  }

  /** Creates an [[UnboundHeader]]. See [[Header]].
    */
  def apply(headerName: String) = UnboundHeader(headerName)
  def unapply(header: Header): Option[(String, HeaderValue)] =
    Some((header.name, header.value))
}
