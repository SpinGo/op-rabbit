package com.spingo.op_rabbit

import java.nio.charset.Charset

/**
  == BATTERIES NOT INCLUDED ==

  To use this package, you must add `'op-rabbit-upickle'` to your dependencies.

  This package lets you marshall and umarshall messages either as JSON or binary MessagePack
  by using upickle.

  == Overview ==

  If you are using the `upickle.default` API then just choose one of the following two
  imports:

  {{{
  // Serialize messages as JSON. UTF-8 encoded with content-type: application/json.
  import com.spingo.op_rabbit.upickleSupport.ujsonDefault._
  }}}

  or

  {{{
  // Serialize messages as MessagePack binary. content-type: application/msgpack.
  import com.spingo.op_rabbit.upickleSupport.upackDefault._
  }}}

  Example:

  {{{
  import com.spingo.op_rabbit.upickleSupport.ujsonDefault._

  case class Person(name: String, age: Int)
  object Person {
    import upickle.default.{ReadWriter => RW, macroRW}
    implicit val rw: RW[Person] = macroRW
  }

  // Both of these can be implicitly created:
  // - implicitly[RabbitMarshaller[Person]]
  // - implicitly[RabbitUnmarshaller[Person]]

  val consumer = AsyncAckingConsumer[Person]("PurplePeopleEater") { person =>
    Future { eat(person) }
  }

  val message = Message.queue(Person("Bill", 25), "people-for-consumption")

  }}}

  === Custom upickle API ===

  If you have a custom `upickle.Api`, for example for handling special serialization of nulls,
  numbers or the like. You must first instantiate a new support object and import from it.

  Example:
  {{{
  import com.spingo.op_rabbit.uPickleSupport

  // For serializing with ujson
  val customJsonSupport = uPickleSupport.ujsonApi(MyCustomPickle)
  import customJsonSupport._


  // For serializing with upack
  val customMsgpackSupport = uPickleSupport.upackApi(MyCustomPickle)
  import customMsgpackSupport._

  }}}
  */
object upickleSupport extends  {

  class ujsonSupport[A <: upickle.Api] private[op_rabbit] (private val api: A) {
    private val CHARSET_UTF_8 = Charset.forName("UTF-8")
    private val CONTENT_TYPE_APP_JSON = "application/json"
    private val ALLOWED_CONTENT_TYPES = Seq(CONTENT_TYPE_APP_JSON, "text/json")


    implicit def marshaller[T](implicit writer: api.Writer[T]): RabbitMarshaller[T] = new RabbitMarshaller[T] {
      override protected val contentType = CONTENT_TYPE_APP_JSON
      override protected val contentEncoding = Some(CHARSET_UTF_8.name)
      override def marshall(value: T) = api.write(value).getBytes(CHARSET_UTF_8)
    }

    implicit def unmarshaller[T](implicit reader: api.Reader[T], manifest: Manifest[T]): RabbitUnmarshaller[T] = new RabbitUnmarshaller[T] {
      override def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): T = contentType match {
        case Some(value) if !contentType.exists(ALLOWED_CONTENT_TYPES.contains) =>
          throw MismatchedContentType(value, CONTENT_TYPE_APP_JSON)
        case _ =>
          api.read[T](new String(value, charset.map(Charset.forName).getOrElse(CHARSET_UTF_8)))
      }
    }
  }

  class upackSupport[A <: upickle.Api] private[op_rabbit] (private val api: A) {
    private val CONTENT_TYPE_MSGPACK = "application/msgpack"
    private val ALLOWED_CONTENT_TYPES = Seq(CONTENT_TYPE_MSGPACK, "application/x-msgpack", "application/octet-stream")

    implicit def marshaller[T](implicit writer: api.Writer[T]): RabbitMarshaller[T] = new RabbitMarshaller[T] {
      override protected val contentType = CONTENT_TYPE_MSGPACK
      override protected def contentEncoding: Option[String] = None
      override def marshall(value: T) = api.writeBinary(value)
    }

    implicit def unmarshaller[T](implicit reader: api.Reader[T], manifest: Manifest[T]): RabbitUnmarshaller[T] = new RabbitUnmarshaller[T] {
      override def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): T = contentType match {
        case Some(value) if !contentType.exists(ALLOWED_CONTENT_TYPES.contains) =>
          throw MismatchedContentType(value, CONTENT_TYPE_MSGPACK)
        case _ =>
          api.readBinary[T](value)
      }
    }
  }

  object ujsonDefault extends ujsonSupport(upickle.default)
  def ujsonApi[A <: upickle.Api](api: A) = new ujsonSupport(api)

  object upackDefault extends upackSupport(upickle.default)
  def upackApi[A <: upickle.Api](api: A) = new upackSupport(api)

}
