package com.spingo.op_rabbit

/**
  Json4s integration using json4s-native; if you are using json4s-jackson, use Json4sJacksonSupport, instead, lest you get a runtime error.
  */
object Json4sSupport extends BaseJson4sSupport {
  val serialization = org.json4s.native.Serialization
}
