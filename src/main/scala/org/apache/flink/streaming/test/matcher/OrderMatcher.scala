/*
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.apache.flink.streaming.test.matcher

import scala.collection.mutable.ArrayBuffer

/**
 * Companion object
 */
object OrderMatcher {
  /**
   * Helper method to create a [[OrderMatcher]] from a [[ListMatcherBuilder]]
   * @param builder to use for creation.
   * @tparam T matched type
   * @return [[OrderMatcher]]
   */
  def createFromBuilder[T](builder: ListMatcherBuilder[T]) : OrderMatcher[T] = {
    new OrderMatcher[T](builder.getConstraints,builder.right)
  }
}

/**
 * Extends [[PartialMatcher]] to test the order of records in an list.
 * @param constraints list of [[ListMatcher]] that define the current expectations
 * @param right list of expected elements
 * @tparam T
 */
class OrderMatcher[T](val constraints: ArrayBuffer[ListMatcher[T]],
                      right : List[T])
  extends PartialMatcher[T](constraints,right) {

  override def getConcreteMatcher(right: List[T]): ListMatcher[T] = {
    ListMatchers.containsInOrder(right)
  }

}
