/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.test.tool.matcher.partial

import org.apache.flink.streaming.test.tool.matcher.ListMatcher
import org.hamcrest.Description
import org.scalatest.exceptions.TestFailedException

import scala.collection.mutable.ArrayBuffer

/**
 * This class is used to ensure the syntax from(Int).to(Int) for creating
 * defining a slice of the expected list of output.
 * @param constraints list of current expectations
 * @param from Integer value marking the start of the slice.
 * @param right expected output.
 * @tparam T
 */
abstract class FromPartialMatcher[T](constraints: ArrayBuffer[ListMatcher[T]],
                           from: Int, right: List[T])
  extends ListMatcher[T](right) {

  /*
  Add a matcher for the current slice of the expected output
   */
  val back = right.splitAt(from)._2
  constraints += getMatcher(back)


  def to(position: Int): Unit = {
    require(position >= 0,"to parameter can not be less than 0")
    require(position < back.length + from, "to parameter index out of bounds")
    require(position > from, "to must be greater than from")

    //remove the current matcher from the back of the list
    constraints.remove(constraints.size-1)
    //add a matcher for the slice restricted by the 'n' value
    val front = back.splitAt(position - from + 1)._1
    constraints += getMatcher(front)
  }

  /**
   * Implement this method to define which [[ListMatcher]] shall
   * be used to check the output.
   * @param right list of expected output
   * @return
   */
  def getMatcher(right: List[T]): ListMatcher[T]

  /**
   * Checks if the list matches expectation
   * @throws TestFailedException if the predicate does not match
   */
  override def matchesSafely(left: List[T]): Boolean = {
    constraints.map{
      _.matchesSafely(left)
    }.reduce(_ && _)
  }

  override def describeTo(description: Description) : Unit = {

  }

}
