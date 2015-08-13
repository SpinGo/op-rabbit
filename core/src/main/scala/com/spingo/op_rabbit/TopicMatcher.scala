package com.spingo.op_rabbit

import scala.annotation.tailrec
import scala.util.matching.Regex

class TopicMatcher protected [op_rabbit] (pattern: List[TopicMatcher.Pattern]) {

  private def iter(pieces: List[String], remainingPattern: List[TopicMatcher.Pattern], matches: List[String] = List.empty): Option[List[String]] = {
    remainingPattern match {
    case Nil if (pieces.isEmpty) =>
      Some(matches)
    case matcher :: rest =>
      matcher.consumeRange(pieces) match {
        case Some(range) =>
          range.reverse.toIterator.map { consume =>
            iter(pieces.drop(consume), rest, if (matcher.produce) (pieces.take(consume).mkString(".") :: matches) else matches)
          }.collectFirst { case Some(result) => result }
        case None => // Fail
          None
      }
    case _ =>
      None
  }}

  def unapply(routingKey: String): Option[List[String]] = {
    iter(if(routingKey == "") List.empty else routingKey.split("\\.", -1).toList, pattern).map(_.reverse)
  }
}

object TopicMatcher {
  trait Pattern {
    def consumeRange(pieces: List[String]): Option[Range]
    val produce: Boolean
  }
  case class Literal(s: String) extends Pattern {
    private val one = Some(1 to 1)
    val produce = false
    def consumeRange(pieces: List[String]) = {
      if ((pieces.length > 0) && (pieces.head == s)) one else None
    }
  }
  case object Word extends Pattern {
    private val one = Some(1 to 1)
    val produce = true
    def consumeRange(pieces: List[String]) = {
      if (pieces.length == 0) None else one
    }
  }
  case object ZeroOrMore extends Pattern {
    val produce = true
    def consumeRange(pieces: List[String]) =
      Some(0 to pieces.length)
  }

  def apply(s: String) = {
    new TopicMatcher(s.split("\\.", -1).map {
      case "*" => Word
      case "#" => ZeroOrMore
      case s => Literal(s)
    }.toList)
  }
  // object WordMatcher {
  //   val asteriskMatcher = "(^|\\.)(\\*)".r
  //   def unapply(s: String): Option[(String, String)] = {
  //     asteriskMatcher.findFirstMatchIn(s) map { m =>
  //       val idx = m.start(2)
  //       (s.take(idx), s.slice(idx + 1, s.length))
  //     }
  //   }
  // }
  // object RestMatcher {
  //   def unapply(s: String): Option[(String)] = {
  //     val i = s.indexOf("#")
  //     if (i > 0) Some(s.take(i)) else None
  //   }
  // }
  // def apply(routingKey: String): TopicMatcher = {
  //   @tailrec def iter(remaining: String, result: String = ""): String = remaining match {
  //     case "" =>
  //       result
  //     case WordMatcher(head, tail) =>
  //       iter(tail, result + Regex.quote(head) + "([^.]*)")
  //     case RestMatcher(head) =>
  //       result + Regex.quote(head) + "(.*)"
  //     case tail =>
  //       result + Regex.quote(tail)
  //   }

  //   new TopicMatcher(("^" + iter(routingKey) + "$").r)
  // }
}
