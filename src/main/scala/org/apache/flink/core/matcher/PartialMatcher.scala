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

import org.hamcrest.Description
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Helps defining matcher that only work with a part of the expected output list.
 * @param constraints list of current [[ListMatcher]]s.
 * @param right list of expected output.
 * @tparam T
 */
abstract class PartialMatcher[T](constraints: ArrayBuffer[ListMatcher[T]], right: List[T])
  extends ListMatcher[T](right) {

  /**
   * Compares the whole list of expected output with the actual output.
   */
  def all() : Unit = {
    constraints += getConcreteMatcher(right)
  }

  /**
   * Uses the list of output starting at a certain position to match the
   * actual output.
   * @param position to split at
   * @return a [[FromPartialMatcher]] implementing the logic and
   *         providing further methods to refine expectations.
   */
  def from(position: Int): FromPartialMatcher[T] = {
    require(position >= 0, "from parameter can not be less than 0")
    require(position < right.length, "from parameter index out of bounds")
    new FromPartialMatcher[T](constraints, position, right) {
      override def getMatcher(right: List[T]): ListMatcher[T] = {
        getConcreteMatcher(right)
      }
    }
  }

  /**
   * Matches the list of expected output to a certain position
   * with the actual output
   * @param position
   */
  def to(position: Int) : Unit = {
    require(position >= 0, "to parameter can not be less than 0")
    require(position < right.length, "to parameter index out of bounds")
    val front = right.splitAt(position + 1)._1
    constraints += getConcreteMatcher(front)
  }

  /**
   * Matches only certain elements denoted by their position in the expected output list
   * with the actual output.
   * @param first position of the first element.
   * @param second position of the second element.
   * @param rest seq of positions of the rest elements.
   */
  def indices(first: Int, second: Int, rest: Int*) : Unit = {
    val relevant = right(first) :: right(second) :: rest.map(right(_)).toList
    constraints += getConcreteMatcher(relevant)
  }

  /**
   * Matches only certain elements denoted by their position in the expected output list
   * with the actual output.
   * @param list of positions of the elements.
   */
  def indices(list: java.util.List[Integer]) : Unit = {
    val relevant = list.map(right(_)).toList
    constraints += getConcreteMatcher(relevant)
  }

  /**
   * Implement this method to define the concrete [[ListMatcher]] that
   * will be used to check the partial lists defined with this class.
   * @param right list to match
   * @return [[ListMatcher]] used by the concrete class.
   */
  def getConcreteMatcher(right: List[T]): ListMatcher[T]

  /**
   * Checks if the list matches the expectations.
   * @throws TestFailedException if the predicate does not match
   */
  override def matchesSafely(left: List[T]): Boolean = {
    constraints.map { (t) =>
      t.matchesSafely(left)
    }.reduce(_ && _)
  }

  override def describeTo(description: Description): Unit = {

  }
}
