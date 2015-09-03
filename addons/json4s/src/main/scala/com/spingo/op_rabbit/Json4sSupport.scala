package com.spingo.op_rabbit

import java.nio.charset.Charset
/**
  == BATTERIES NOT INCLUDED ==

  To use this package, you must add `'op-rabbit-json4s'` to your dependencies.

  == Using ==

  If using `json4s-native`, `import Json4sSupport.native._`

  If using `json4s-jackson`, `import Json4sSupport.jackson._`
  */
object Json4sSupport {
  import org.json4s._
  protected trait BaseJson4sSupport {
    val serialization: org.json4s.Serialization

    private val utf8 = Charset.forName("UTF-8")
    implicit def json4sRabbitMarshaller[T <: AnyRef](implicit formats: Formats): RabbitMarshaller[T] = {
      new RabbitMarshaller[T] {
        protected val contentType = "application/json"
        private val encoding = "UTF-8"
        protected val contentEncoding = Some(encoding)
        def marshall(value: T) =
          serialization.write(value).toString.getBytes(utf8)
      }
    }

    implicit def json4sRabbitUnmarshaller[T <: AnyRef](implicit formats: Formats, manifest: Manifest[T]): RabbitUnmarshaller[T] = {
      new RabbitUnmarshaller[T] {
        def unmarshall(value: Array[Byte], contentType: Option[String], charset: Option[String]): T = {
          contentType match {
            case Some(value) if (value != "application/json" && value != "text/json") =>
              throw MismatchedContentType(value, "application/json")
            case _ =>
              serialization.read[T](new String(value, charset map (Charset.forName) getOrElse utf8))
          }
        }
      }
    }
  }

  /**
    Contains an implicit [[RabbitMarshaller]] and implicit [[RabbitUnmarshaller]] which uses `json4s-native`
    */
  lazy val native = new BaseJson4sSupport {
    val serialization = org.json4s.native.Serialization
  }

  /**
    Contains an implicit [[RabbitMarshaller]] and implicit [[RabbitUnmarshaller]] which uses `json4s-jackson`
    */
  lazy val jackson = new BaseJson4sSupport {
    val serialization = org.json4s.jackson.Serialization
  }
}
