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
package org.flinkspector.matcher

import org.hamcrest.{Description, TypeSafeDiagnosingMatcher}
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * This class is used to define expectations to verify test output.
 * The MatcherBuilder generates a list of [[ListMatcher]]'s which it will
 * use to check the output.
 * @tparam T
 */
class ListMatcherBuilder[T](val right: List[T]) extends TypeSafeDiagnosingMatcher[List[T]] {

  /** List of [[ListMatcher]]s to define expectations  */
  private val constraints: ArrayBuffer[ListMatcher[T]] = new ArrayBuffer[ListMatcher[T]]()

  private var onlySelected = false

  def this(jList: java.util.List[T]) {
    this(jList.toList)
  }

  /*
    CLOSE OF CONSTRUCTOR
   */

  //  def all() : MatcherBuilder[T] = {
  //    constraints += ListMatchers.containsAll[T](right)
  //    this
  //  }

  /**
   * Tests whether the output contains only the expected records.
   */
  def only() = {
    constraints += ListMatchers.containsOnly[T](right)
    onlySelected = true
    this
  }

  /**
   * Tests whether the output contains no duplicates in reference
   * to the expected output.
   */
  def noDuplicates() = {
    constraints += ListMatchers.containsNoDuplicates[T](right)
    this
  }

  /**
   * Provides a matcher to verify the order of 
   * elements in the output.
   */
  def inOrder(): OrderMatcher[T] = {
    new OrderMatcher[T](constraints, right)
  }

  /**
   * Provides a matcher to verify the order of
   * elements in the output.
   */
  def inSeries(): SeriesMatcher[T] = {
    new SeriesMatcher[T](constraints, right)
  }

  /**
   * Getter for the list of contraints.
   * @return array of [[ListMatcher]]
   */
  def getConstraints: ArrayBuffer[ListMatcher[T]] = {
    constraints
  }

  def validate(output: java.lang.Iterable[T]): Boolean = {
    matches(output.toList)
  }

  /**
   * Checks if the list matches expectation.
   * @param left actual output.
   * @throws TestFailedException if the predicate does not match.
   */
  override def matchesSafely(left: scala.List[T], mismatch: Description): Boolean = {
    if (constraints.isEmpty || !onlySelected) {
      constraints += ListMatchers.containsAll[T](right)
    }

    constraints.foreach { m =>
      if (!m.matches(left)) {
        m.describeMismatch(left, mismatch)
        return false
      }
    }
    true
  }

  override def describeTo(description: Description): Unit = {
    description.appendText("output ( ")
    constraints
      .filter(_.toString != "order matcher")
      .foreach { m =>
        description.appendDescriptionOf(m)
        description.appendText(" ")
      }
    description.appendValue(right)
    val orderMatcher = constraints
      .filterNot(_.toString != "order matcher")
    if(orderMatcher.nonEmpty) {
      description.appendText(" and ")
      orderMatcher.foreach { m =>
        description.appendDescriptionOf(m)
        description.appendText(" ")
      }
    }

    description.appendText(") ")
  }
}



