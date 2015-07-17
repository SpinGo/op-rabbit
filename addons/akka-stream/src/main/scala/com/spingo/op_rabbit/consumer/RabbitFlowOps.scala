package com.spingo.op_rabbit.consumer

import akka.event.LoggingAdapter
import akka.stream.{Graph, Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import com.spingo.op_rabbit.SameThreadExecutionContext
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.{GenSeqLike, immutable}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

abstract class RabbitFlowOps[+Out, +Mat] extends AnyRef {
  type UnwrappedRepr[+O, +M] <: akka.stream.scaladsl.FlowOps[O, M]
  type WrappedRepr[+O, +M] <: akka.stream.scaladsl.FlowOps[(Promise[Unit], O), M]
  type Repr[+O, +M] <: RabbitFlowOps[O, M]

  protected val wrappedRepr: WrappedRepr[Out, Mat]
  def collect[T](pf: PartialFunction[Out, T]): Repr[T, Mat] =
    andThen {
      wrappedRepr.mapConcat { case (p, data) =>
        if (pf.isDefinedAt(data)) {
          List((p, propException(p)(pf(data))))
        } else {
          p.success(())
          List.empty
        }
      }
    }

  /**
    See FlowOps.groupedWithin

    Downstream acknowledgement applies to the resulting group (IE: if it yields a group of 100, then downstream you can only either ack or nack the entire group)
    */
  def groupedWithin(n: Int, d: FiniteDuration): Repr[immutable.Seq[Out], Mat] = {
    andThenCombine { wrappedRepr.groupedWithin(n, d) }
  }

  /**
    See FlowOps.buffer; does not accept an OverflowStrategy because only backpressure and fail are supported.
    */
  def buffer(size: Int, failOnOverflow: Boolean = false): Repr[Out, Mat] = andThen {
    wrappedRepr.buffer(size, if (failOnOverflow) OverflowStrategy.fail else OverflowStrategy.backpressure)
  }
  /**
    See FlowOps.grouped

    Downstream acknowledgement applies to the resulting group (IE: if it yields a group of 100, then downstream you can only either ack or nack the entire group)
    */
  def grouped(n: Int): Repr[immutable.Seq[Out], Mat] = {
    andThenCombine { wrappedRepr.grouped(n) }
  }

  /**
    See FlowOps.mapConcat

    Splits a single delivery into 0 or more items. If 0 items, then signal completion of this message. Otherwise, signal completion of this message after all resulting items are signaled for completion.
    */
  def mapConcat[T](f: Out ⇒ immutable.Iterable[T]): Repr[T, Mat] = andThen {
    wrappedRepr.mapConcat { case (p, data) =>
      val items = Stream.continually(Promise[Unit]) zip propException(p)(f(data))
      if (items.length == 0) {
        p.success(()) // effectively a filter. We're done with this message.
          items
      } else {
        implicit val ec = SameThreadExecutionContext
        p.completeWith(Future.sequence(items.map(_._1.future)).map(_ => ()))
        items
      }
    }
  }

  // Yields an Unwrapped Repr with only the data; after this point, message are acknowledged
  def acked = wrappedRepr.map { case (p, data) =>
    p.success(())
    data
  }

  // Yields an unacked Repr with the promise and the data.
  def unsafe = wrappedRepr

  /**
    See FlowOps.groupBy
    */
  def groupBy[K, U >: Out](f: (Out) ⇒ K): wrappedRepr.Repr[(K, RabbitSource[U, Unit]), Mat] = {
    wrappedRepr.groupBy { case (p, o) => propException(p) { f(o) } }.map { case (key, flow) =>
      (key, new RabbitSource(flow))
    }
  }

  /**
    See FlowOps.filter
    */
  def filter(predicate: (Out) ⇒ Boolean): Repr[Out, Mat] = andThen {
    wrappedRepr.filter { case (p, data) =>
      val result = (propException(p)(predicate(data)))
      if (!result) p.success(())
      result
    }
  }

  /**
    See FlowOps.log
    */
  def log(name: String, extract: (Out) ⇒ Any = identity)(implicit log: LoggingAdapter = null): Repr[Out, Mat] = andThen {
    wrappedRepr.log(name, { case (p, d) => propException(p) { extract(d) }})
  }

  /**
    See FlowOps.map
    */
  def map[T](f: Out ⇒ T): Repr[T, Mat] = andThen {
    wrappedRepr.map { case (p, d) =>
      implicit val ec = SameThreadExecutionContext
      (p, propException(p)(f(d)))
    }
  }

  /**
    See FlowOps.mapAsync
    */
  def mapAsync[T](parallelism: Int)(f: Out ⇒ Future[T]): Repr[T, Mat] = andThen {
    wrappedRepr.mapAsync(parallelism) { case (p, d) =>
      implicit val ec = SameThreadExecutionContext
      propFutureException(p)(f(d)) map { r => (p, r) }
    }
  }

  /**
    See FlowOps.mapAsyncUnordered
    */
  def mapAsyncUnordered[T](parallelism: Int)(f: Out ⇒ Future[T]): Repr[T, Mat] = andThen {
    wrappedRepr.mapAsyncUnordered(parallelism) { case (p, d) =>
      implicit val ec = SameThreadExecutionContext
      propFutureException(p)(f(d)) map { r => (p, r) }
    }
  }

  /**
    See FlowOps.conflate

    Conflated items are grouped together into a single message, the acknowledgement of which acknowledges every message that went into the group.
    */
  def conflate[S](seed: (Out) ⇒ S)(aggregate: (S, Out) ⇒ S): Repr[S, Mat] = andThen {
    wrappedRepr.conflate({ case (p, data) => (p, propException(p)(seed(data))) }) { case ((seedPromise, seedData), (p, element)) =>
      seedPromise.completeWith(p.future)
      (p, propException(p)(aggregate(seedData, element)))
    }
  }

  // propagate exception, doesn't recover
  private def propFutureException[T](p: Promise[Unit])(f: => Future[T]): Future[T] = {
    implicit val ec = SameThreadExecutionContext
    propException(p)(f).onFailure { case e => p.tryFailure(e) }
    f
  }

  // Catch and propagate exception; exception is still thrown
  // TODO - rather than catching the exception, wrap it, with the promise, and wrap the provided handler. If the handler is invoked, then nack the message with the exception. This way, .recover can be supported.
  private def propException[T](p: Promise[Unit])(t: => T): T = {
    try {
      t
    } catch {
      case e: Throwable =>
        p.failure(e)
        throw(e)
    }
  }

  protected def andThen[U, Mat2 >: Mat](next: WrappedRepr[(Promise[Unit], U), Mat2]): Repr[U, Mat2]

  // The compiler needs a little bit of help to know that this conversion is possible
  private implicit def collapse2to1[U, Mat2 >: Mat](next: wrappedRepr.Repr[_, _]#Repr[U, Mat2]): wrappedRepr.Repr[U, Mat2] = next.asInstanceOf[wrappedRepr.Repr[U, Mat2]]
  private implicit def collapse2to0[U, Mat2 >: Mat](next: wrappedRepr.Repr[_, _]#Repr[U, Mat2]): WrappedRepr[U, Mat2] = next.asInstanceOf[WrappedRepr[U, Mat2]]
  implicit def collapse1to0[U, Mat2 >: Mat](next: wrappedRepr.Repr[_, _]): WrappedRepr[U, Mat2] = next.asInstanceOf[WrappedRepr[U, Mat2]]

  // Combine all promises into one, such that the fulfillment of that promise fulfills the entire group
  private def andThenCombine[U, Mat2 >: Mat](next: wrappedRepr.Repr[immutable.Seq[(Promise[Unit], U)], Mat2]): Repr[immutable.Seq[U], Mat2] =
    andThen {
      next.map { data =>
        (
          data.map(_._1).reduce { (p1, p2) => p1.completeWith(p2.future); p2 },
          data.map(_._2)
        )
      }
    }
}

class RabbitFlow[-In, +Out, +Mat](val wrappedRepr: Flow[(Promise[Unit], In), (Promise[Unit], Out), Mat]) extends RabbitFlowOps[Out, Mat] {
  type UnwrappedRepr[+O, +M] = Flow[In @uncheckedVariance, O, M]
  type WrappedRepr[+O, +M] = Flow[(Promise[Unit], In @uncheckedVariance), (Promise[Unit], O), M]
  type Repr[+O, +M] = RabbitFlow[In @uncheckedVariance, O, M]

  protected def andThen[U, Mat2 >: Mat](next: WrappedRepr[(Promise[Unit], U), Mat2] @uncheckedVariance): Repr[U, Mat2] = {
    new RabbitFlow(next)
  }
}

object RabbitFlow {
  def apply[T] = new RabbitFlow(Flow.apply[(Promise[Unit], T)])

  def apply[In, Out, Mat](wrappedFlow: Flow[(Promise[Unit], In), (Promise[Unit], Out), Mat]) = new RabbitFlow(wrappedFlow)
}
