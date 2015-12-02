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

package org.flinkspector.core.matcher

import org.flinkspector.core.CoreSpec
import org.flinkspector.matcher.{PartialMatcher, ListMatcher}
import org.mockito.Mockito._

import scala.collection.mutable.ArrayBuffer

class PartialMatcherSpec extends CoreSpec {

  /**
   * test case setup
   */
  trait PartialMatcherCase {
    val listMatcher = mock[ListMatcher[Int]]
    when(listMatcher.matchesSafely(List())).thenReturn(true)
    var selectedList: List[Int] = _
    //the list that gets produced by the partial matcher
    val right = List(1, 2, 3, 4, 5, 6)
    val matcher = new PartialMatcher[Int](ArrayBuffer.empty[ListMatcher[Int]], right) {
      override def getConcreteMatcher(right: scala.List[Int]): ListMatcher[Int] = {
        //memorize handed list for checks
        selectedList = right
        listMatcher
      }
    }
  }

  /*
  TESTS
   */

  "the partial matcher" should "select the whole list" in new PartialMatcherCase {
    matcher.all()
    selectedList should equal(List(1, 2, 3, 4, 5, 6))
  }

  it should "select the back of a list" in new PartialMatcherCase {
    matcher.from(3)
    selectedList should equal(List(4, 5, 6))
  }

  it should "select the front of a list" in new PartialMatcherCase {
    matcher.to(2)
    selectedList should equal(List(1, 2, 3))
  }

  it should "select certain elements of a list" in new PartialMatcherCase {
    matcher.indices(1, 3, 5)
    selectedList should equal(List(2, 4, 6))
  }

  it should "select a part of a list" in new PartialMatcherCase {
    matcher.from(1).to(4)
    selectedList should equal(List(2, 3, 4, 5))
  }

  it should "select a smaller part of a list" in new PartialMatcherCase {
    matcher.from(2).to(4)
    selectedList should equal(List(3, 4, 5))
  }

  it should "select the last elements of a list" in new PartialMatcherCase {
    matcher.from(2).to(5)
    selectedList should equal(List(3, 4, 5, 6))
  }

  it should "test only once for one requirement" in new PartialMatcherCase {
    matcher.from(2).to(5)
    matcher.matchesSafely(List())

    verify(listMatcher).matchesSafely(List())
  }

  it should "test only two times for two requirements" in new PartialMatcherCase {
    matcher.from(2).to(5)
    matcher.all()
    matcher.matchesSafely(List())
    verify(listMatcher, times(2)).matchesSafely(List())
  }

  it should "test only two times for two requirements (permutation)" in new PartialMatcherCase {
    matcher.all()
    matcher.from(2).to(5)
    matcher.matchesSafely(List())
    verify(listMatcher, times(2)).matchesSafely(List())
  }

  it should "test only two times for two requirements (alternation)" in new PartialMatcherCase {

    matcher.from(1).to(3)
    matcher.from(2).to(5)
    matcher.matchesSafely(List())

    verify(listMatcher, times(2)).matchesSafely(List())
  }


}
