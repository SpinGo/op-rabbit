/*
 The majority of this code was inspired / copied from the spray project, with changes to suit the domain of op_rabbit
 */
/*
 * Copyright © 2011-2015 the spray project <http://spray.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spingo.op_rabbit

import shapeless._
import com.spingo.op_rabbit.properties.{HeaderValue, NullHeaderValue, HeaderValueConverter}

private [op_rabbit] trait HListDeserializer[L <: HList, T] extends Deserializer[L, T] {
  def apply(data: L): Either[ExtractRejection, T]
}
private [op_rabbit] object HListDeserializer extends HListDeserializerInstances {
  protected type DS[A, AA] = Deserializer[A, AA] // alias for brevity

  implicit def holderToHList[L <: HList, T](holder: TypeHolder[T])(implicit hv: HListDeserializer[L, T]) = hv

  /////////////////////////////// CASE CLASS DESERIALIZATION ////////////////////////////////

  // we use a special exception to bubble up errors rather than relying on long "right.flatMap" cascades in order to
  // save lines of code as well as excessive closure class creation in the many "hld" methods below
  private class BubbleLeftException(val left: Left[Any, Any]) extends RuntimeException
  protected def create[L <: HList, T](deserialize: L ⇒ T): HListDeserializer[L, T] =
    new HListDeserializer[L, T] {
      def apply(list: L): Deserialized[T] =
        try Right(deserialize(list))
        catch {
          case e: BubbleLeftException      ⇒ e.left.asInstanceOf[Left[ExtractRejection, T]]
          case e: IllegalArgumentException ⇒ Left(ParseExtractRejection(Option(e.getMessage) getOrElse "", e))
        }
    }

  protected def get[T](either: Deserialized[T]): T = either match {
    case Right(x)         ⇒ x
    case left: Left[_, _] ⇒ throw new BubbleLeftException(left)
  }
}
