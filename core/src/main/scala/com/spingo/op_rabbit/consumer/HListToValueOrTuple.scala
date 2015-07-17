package com.spingo.op_rabbit.consumer

import shapeless._

trait HListToValueOrTuple[L <: HList] {
  type Out
  def apply(hl: L): Out
}

object HListToValueOrTuple extends HListToValueOrTupleInstances {
  implicit val hnilTupler: Aux[HNil, Unit] =
    new HListToValueOrTuple[HNil] {
      type Out = Unit
      def apply(l: HNil): Out = ()
    }

  implicit def hlistTupler1[A]:       Aux[A :: HNil, A] = new HListToValueOrTuple[A :: HNil] { type Out = A;         def apply(l: A :: HNil): Out           = l match { case a :: HNil           => a } }
}
