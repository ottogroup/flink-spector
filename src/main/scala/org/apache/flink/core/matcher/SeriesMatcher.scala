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
package org.apache.flink.core.matcher

import scala.collection.mutable.ArrayBuffer

/**
 * Companion object
 */
object SeriesMatcher {

  /**
   * Helper function to create a [[SeriesMatcher]] from a [[ListMatcherBuilder]]
   * @param builder to use for creation.
   * @tparam T
   * @return [[SeriesMatcher]]
   */
  def createFromBuilder[T](builder: ListMatcherBuilder[T]): SeriesMatcher[T] = {
    new SeriesMatcher[T](builder.getConstraints, builder.right)
  }
}

/**
 * Extends [[PartialMatcher]] to create a matcher, which tests
 * whether a list is contained in the output.
 * @param constraints list of [[ListMatcher]] that define the current expectations.
 * @param right list of expected elements
 * @tparam T
 */
class SeriesMatcher[T](constraints: ArrayBuffer[ListMatcher[T]],
                       right: List[T])
  extends PartialMatcher[T](constraints, right) {

  override def getConcreteMatcher(right: List[T]): ListMatcher[T] = {
    ListMatchers.containsInSeries(right)
  }
}
