package com.spingo.op_rabbit

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.annotation.unchecked.uncheckedVariance
import scala.util.{Success, Try}
import shapeless._
import shapeless.ops.hlist.Prepend

/**
  HListable
  */
private [op_rabbit] trait ConjunctionMagnet[L <: HList] {
  type Out
  def apply(underlying: Directive[L]): Out
}

private [op_rabbit] object ConjunctionMagnet {
  implicit def fromDirective[L <: HList, R <: HList](other: Directive[R])(implicit p: Prepend[L, R]) =
    new ConjunctionMagnet[L] {
      type Out = Directive[p.Out]
      def apply(underlying: Directive[L]): Out =
        new Directive[p.Out] {
          def happly(f: p.Out ⇒ Handler) =
            underlying.happly { prefix ⇒
              other.happly { suffix ⇒
                f(prefix ++ suffix)
              }
            }
        }
    }
}

abstract class Directive[+L <: HList] { self =>
  def &(magnet: ConjunctionMagnet[L] @uncheckedVariance): magnet.Out = magnet(this)
  def as[T](deserializer: HListDeserializer[L, T] @uncheckedVariance): Directive1[T] = new Directive1[T] {
    def happly(f: ::[T, HNil] => Handler): Handler = {
      self.happly { l =>
        { (p, delivery) =>
          deserializer.apply(l) match {
            case Left(rejection) => p.failure(rejection)
            case Right(result) =>
              f(result :: HNil)(p, delivery)
          }
        }
      }
    }
  }
  def |[R >: L <: HList](that: Directive[R]): Directive[R] =
    new Directive[R] {
      def happly(f: R => Handler) = { (upstreamPromise, delivery) =>
        @volatile var doRecover = true
        val interimPromise = Promise[Result]
        val left = self.happly { list =>
          { (promise, delivery) =>
            // if we made it this far, then the directives succeeded; don't recover
            doRecover = false
            f(list)(promise, delivery)
          }
        }(interimPromise, delivery)
        import scala.concurrent.ExecutionContext.Implicits.global
        interimPromise.future.onComplete {
          case Success(Left(rejection)) if doRecover =>
            that.happly(f)(upstreamPromise, delivery)
          case _ =>
            upstreamPromise.completeWith(interimPromise.future)
        }
      }
    }

  def hmap[R](f: L => R)(implicit hl: HListable[R]): Directive[hl.Out] =
    new Directive[hl.Out] {
      def happly(g: hl.Out => Handler) = self.happly { values => g(hl(f(values))) }
    }
  def hflatMap[R <: HList](f: L => Directive[R]): Directive[R] = {
    new Directive[R] {
      def happly(g: R => Handler) = self.happly { values => f(values).happly(g) }
    }
  }

  def happly(f: L => Handler): Handler
}
object Directive {
  implicit def pimpApply[L <: HList](directive: Directive[L])(implicit hac: ApplyConverter[L]): hac.In ⇒ Handler = f ⇒ directive.happly(hac(f))

  implicit class SingleValueModifiers[T](underlying: Directive1[T]) {
    def map[R](f: T ⇒ R)(implicit hl: HListable[R]): Directive[hl.Out] =
      underlying.hmap { case value :: HNil ⇒ f(value) }

    def flatMap[R <: HList](f: T ⇒ Directive[R]): Directive[R] =
      underlying.hflatMap { case value :: HNil ⇒ f(value) }
  }
  implicit class OptionDirective[A](directive: Directive1[Option[A]]) {
    def getOrElse[B >: A](value: => B): Directive1[B] =
      directive.map { _ getOrElse value }
  }
}
