package com.spingo.op_rabbit.stream

import akka.pattern.pipe
import akka.stream.{Graph, Materializer}
import akka.stream.scaladsl.{Keep, RunnableGraph, Source}
import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}


class AckedSource[+Out, +Mat](val wrappedRepr: Source[AckTup[Out], Mat]) extends AckedFlowOps[Out, Mat] with AckedGraph[AckedSourceShape[Out], Mat] {
  type UnwrappedRepr[+O, +M] = Source[O, M]
  type WrappedRepr[+O, +M] = Source[AckTup[O], M]
  type Repr[+O, +M] = AckedSource[O, M]

  lazy val shape = new AckedSourceShape(wrappedRepr.shape)
  val akkaGraph = wrappedRepr
  /**
   * Connect this [[akka.stream.scaladsl.Source]] to a [[akka.stream.scaladsl.Sink]],
   * concatenating the processing steps of both.
   */
  def runAckMat[Mat2](combine: (Mat, Future[Unit]) ⇒ Mat2)(implicit materializer: Materializer): Mat2 =
    wrappedRepr.toMat(AckedSink.ack.akkaSink)(combine).run

  def runAck(implicit materializer: Materializer) = runAckMat(Keep.right)

  def runWith[Mat2](sink: AckedSink[Out, Mat2])(implicit materializer: Materializer): Mat2 =
    wrappedRepr.runWith(sink.akkaSink)

  def runFold[U](zero: U)(f: (U, Out) ⇒ U)(implicit materializer: Materializer): Future[U] =
    runWith(AckedSink.fold(zero)(f))

  def runForeach(f: (Out) ⇒ Unit)(implicit materializer: Materializer): Future[Unit] =
    runWith(AckedSink.foreach(f))

  def to[Mat2](sink: AckedSink[Out, Mat2]): RunnableGraph[Mat] =
    wrappedRepr.to(sink.akkaSink)

  def toMat[Mat2, Mat3](sink: AckedSink[Out, Mat2])(combine: (Mat, Mat2) ⇒ Mat3): RunnableGraph[Mat3] =
    wrappedRepr.toMat(sink.akkaSink)(combine)

  protected def andThen[U, Mat2 >: Mat](next: WrappedRepr[U, Mat2]): Repr[U, Mat2] = {
    new AckedSource(next)
  }
}

object AckedSource {
  type OUTPUT[T] = AckTup[T]

  def apply[T](iterable: scala.collection.immutable.Iterable[AckTup[T]]): AckedSource[T, Unit] = {
    new AckedSource(Source(iterable))
  }

  def fromIterableData[T](iterable: scala.collection.immutable.Iterable[T]): AckedSource[T, Unit] = {
    apply(Stream.continually(Promise[Unit]) zip iterable)
  }
}
