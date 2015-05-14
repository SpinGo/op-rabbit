package com.spingo.op_rabbit

/**
  Json4s integration using json4s-native; if you are using json4s-native, use Json4sSupport, instead, lest you get a runtime error.
  */
object Json4sJacksonSupport extends BaseJson4sSupport {
  val serialization = org.json4s.jackson.Serialization
}
