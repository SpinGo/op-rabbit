package com.spingo.op_rabbit.stream
import akka.stream._
import akka.stream.scaladsl._
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Future
import scala.concurrent.Promise

trait AckedShape { self =>
  type Self <: AckedShape
  type AkkaShape <: akka.stream.Shape
  val akkaShape: AkkaShape
  def wrapShape(akkaShape: AkkaShape): Self
}

trait AckedGraph[+S <: AckedShape, +M] {
  type Shape = S @uncheckedVariance
  protected [stream] val shape: Shape
  type AkkaShape = shape.AkkaShape
  val akkaGraph: Graph[AkkaShape, M]
  def wrapShape(akkaShape: akkaGraph.Shape): shape.Self =
    shape.wrapShape(akkaShape)
}

final class AckedSourceShape[+T](s: SourceShape[AckTup[T]]) extends AckedShape {
  type Self = AckedSourceShape[T] @uncheckedVariance
  type AkkaShape = SourceShape[AckTup[T]] @uncheckedVariance
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedSourceShape(akkaShape)
}

final class AckedSinkShape[-T](s: SinkShape[AckTup[T]]) extends AckedShape {
  type Self = AckedSinkShape[T] @uncheckedVariance
  type AkkaShape = SinkShape[AckTup[T]] @uncheckedVariance
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedSinkShape(akkaShape)
}

class AckedFlowShape[-I, +O](s: FlowShape[AckTup[I], AckTup[O]]) extends AckedShape {
  type Self = AckedFlowShape[I, O] @uncheckedVariance
  type AkkaShape = FlowShape[AckTup[I], AckTup[O]] @uncheckedVariance
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedFlowShape(akkaShape)
}

class AckedUniformFanOutShape[I, O](s: UniformFanOutShape[AckTup[I], AckTup[O]]) extends AckedShape {
  type Self = AckedUniformFanOutShape[I, O] @uncheckedVariance
  type AkkaShape = UniformFanOutShape[AckTup[I], AckTup[O]]
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedUniformFanOutShape(akkaShape)
}

class AckedBroadcast[T](g: Graph[UniformFanOutShape[AckTup[T], AckTup[T]], Unit]) extends AckedGraph[AckedUniformFanOutShape[T, T], Unit] {
  val shape = new AckedUniformFanOutShape(g.shape)
  val akkaGraph = g
}

object AckedBroadcast {
  private class AckedBroadcastImpl[T](outputPorts: Int)
      extends FlexiRoute[AckTup[T], UniformFanOutShape[AckTup[T], AckTup[T]]](new UniformFanOutShape(outputPorts), Attributes.name("AckedBroadcast"))
  {
    import FlexiRoute._

    implicit val ec = SameThreadExecutionContext
    override def createRouteLogic(p: PortT) = new RouteLogic[AckTup[T]] {
      override def initialState =
        State[Any](DemandFromAll(p.outlets : _*)) { case (ctx, _, (ackPromise, element)) =>
          val downstreamAckPromises = List.fill(outputPorts)(Promise[Unit])

          (downstreamAckPromises zip p.outlets).foreach { case (downstreamAckPromise, port) =>
            ctx.emit(port.asInstanceOf[Outlet[AckTup[T]]])((downstreamAckPromise, element))
          }
          ackPromise.completeWith(Future.sequence(downstreamAckPromises.map(_.future)).map(_ => ()))
          SameState
        }

      override def initialCompletionHandling = eagerClose
    }
  }

  def apply[T](outputPorts: Int) = {
    val thing = new AckedBroadcastImpl[T](outputPorts)
    new AckedBroadcast[T](thing)
  }
}

object AckedFlowGraph {
  class Builder[+M](implicit val akkaBuilder: FlowGraph.Builder[M]) {
    def addEdge[A, B, M2](from: Outlet[A], via: Graph[FlowShape[A, B], M2], to: Inlet[B]): Unit =
      akkaBuilder.addEdge(from, via, to)

    def add[S <: AckedShape](graph: AckedGraph[S, _]): graph.shape.Self = {
      val s = akkaBuilder.add(graph.akkaGraph)
      graph.wrapShape(s)
    }
  }

  def closed[Mat, M1, M2](g1: AckedGraph[AckedShape, M1], g2: AckedGraph[AckedShape, M2])(combineMat: (M1, M2) ⇒ Mat)(buildBlock: (AckedFlowGraph.Builder[Mat]) ⇒ (g1.Shape, g2.Shape) ⇒ Unit): RunnableGraph[Mat] = {
    FlowGraph.closed(g1.akkaGraph, g2.akkaGraph)(combineMat) { implicit builder =>
      (a, b) =>
      buildBlock(new AckedFlowGraph.Builder)(g1.wrapShape(a), g2.wrapShape(b))
    }
  }

  object Implicits {
    import FlowGraph.Implicits._

    trait AckedCombinerBase[T] {
      implicit val left: CombinerBase[AckTup[T]]

      def ~>[Out](junction: AckedUniformFanOutShape[T, Out])(implicit builder: AckedFlowGraph.Builder[_]): AckedPortOps[Out, Unit] =
        new AckedPortOps(left ~> junction.akkaShape)

      def ~>(sink: AckedSinkShape[T])(implicit builder: AckedFlowGraph.Builder[_]): Unit =
        left ~> sink.akkaShape

      def ~>[Out](flow: AckedFlowShape[T, Out])(implicit builder: AckedFlowGraph.Builder[_]): AckedPortOps[Out, Unit] =
        new AckedPortOps(left ~> flow.akkaShape)

      def ~>(to: AckedGraph[AckedSinkShape[T], _])(implicit builder: AckedFlowGraph.Builder[_]): Unit =
        left ~> to.akkaGraph

      def ~>[Out](via: AckedGraph[AckedFlowShape[T, Out], Any])(implicit builder: AckedFlowGraph.Builder[_]): AckedPortOps[Out, Unit] =
        new AckedPortOps(left ~> via.akkaGraph)
    }

    class AckedPortOps[Out, Mat](val left: PortOps[AckTup[Out], Mat])(implicit val builder: AckedFlowGraph.Builder[_]) extends AckedFlowOps[Out, Mat] with AckedCombinerBase[Out] {
      type UnwrappedRepr[+O, +M] <: PortOps[O, M] @uncheckedVariance
      type WrappedRepr[+O, +M] <: PortOps[AckTup[O], M] @uncheckedVariance
      type Repr[+O, +M] = AckedPortOps[O, M] @uncheckedVariance

      val wrappedRepr: WrappedRepr[Out, Mat] = left

      protected def andThen[U, Mat2 >: Mat](next: WrappedRepr[U, Mat2]): Repr[U, Mat2] =
        new AckedPortOps(next)
    }

    implicit def ackedBuilder2Builder[M](implicit b: AckedFlowGraph.Builder[M]): FlowGraph.Builder[M] = b.akkaBuilder

    implicit class AckedSourceArrow[T](val g: AckedGraph[AckedSourceShape[T], _])(implicit val builder: AckedFlowGraph.Builder[_]) extends AckedCombinerBase[T] {
      val left: CombinerBase[AckTup[T]] = SourceArrow(g.akkaGraph)
    }
    implicit class AckedSourceShapeArrow[T](val s: AckedSourceShape[T])(implicit val builder: AckedFlowGraph.Builder[_]) extends AckedCombinerBase[T] {
      val left: CombinerBase[AckTup[T]] = SourceShapeArrow(s.akkaShape)
    }

    implicit def flow2flow[I, O](f: AckedFlowShape[I, O])(implicit b: AckedFlowGraph.Builder[_]): AckedPortOps[O, Unit] =
      new AckedPortOps(f.akkaShape)

    implicit def ackedFanOut2flow[I, O](j: AckedUniformFanOutShape[I, O])(implicit b: AckedFlowGraph.Builder[_]): AckedPortOps[O, Unit] = {
      new AckedPortOps(j.akkaShape)
    }
  }
}
