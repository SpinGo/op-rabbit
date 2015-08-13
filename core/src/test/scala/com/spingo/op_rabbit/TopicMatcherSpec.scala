package com.spingo.op_rabbit

import org.scalatest.{FunSpec,Matchers}
class RoutingMatcherSpec extends FunSpec with Matchers {
  describe("matching topics") {
    it("matches words at the given locations") {
      val FreshFruit = TopicMatcher("fruit.*.fresh.*")
      FreshFruit.unapply("fruit.apple.fresh.15") should be (Some(List("apple", "15")))
    }

    it("only matches one word for a *") {
      val FreshFruit = TopicMatcher("fruit.*")
      FreshFruit.unapply("fruit.apple") should be (Some(List("apple")))
      FreshFruit.unapply("fruit.apple.fresh") should be (None)
    }

    it("only matches the rest on #") {
      val FreshFruit = TopicMatcher("fruit.#")
      FreshFruit.unapply("fruit.apple") should be (Some(List("apple")))
      FreshFruit.unapply("fruit.apple.fresh") should be (Some(List("apple.fresh")))
    }

    it("doesn't match an empty topic for *") {
      val matcher = TopicMatcher("*")
      matcher.unapply("") should be (None)
    }

    it("doesn't match at all if * not by boundary") {
      // this is how rabbit behaves... it doesn't throw, it just doesn't match
      val matcher = TopicMatcher("w*")
      matcher.unapply("word") should be (None)
      matcher.unapply("w.ord") should be (None)
    }

    it("matches at least the number of words specified after the #") {
      val matcher = TopicMatcher("#.*.*")
      matcher.unapply("word") should be (None)
      matcher.unapply("word.word") should be (Some(List("", "word", "word")))
      matcher.unapply("word.word.word") should be (Some(List("word", "word", "word")))
    }

    it("* matches an empty word") {
      val matcher = TopicMatcher("*.*.*")
      matcher.unapply("..") should be (Some(List("", "", "")))
      matcher.unapply(".") should be (None)
    }
  }
}
