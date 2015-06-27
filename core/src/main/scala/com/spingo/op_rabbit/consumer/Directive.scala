package com.spingo.op_rabbit.consumer

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Success, Try}
import shapeless._
import shapeless.ops.hlist.Prepend

protected trait ConjunctionMagnet[L <: HList] {
  type Out
  def apply(underlying: Directive[L]): Out
}

protected object ConjunctionMagnet {
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

abstract class Directive[L <: HList] { self =>
  def &(magnet: ConjunctionMagnet[L]): magnet.Out = magnet(this)
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
  def happly(f: L => Handler): Handler
}
object Directive {
  implicit def pimpApply[L <: HList](directive: Directive[L])(implicit hac: ApplyConverter[L]): hac.In ⇒ Handler = f ⇒ directive.happly(hac(f))
}

trait Directive1[T] extends Directive[T :: HNil]
