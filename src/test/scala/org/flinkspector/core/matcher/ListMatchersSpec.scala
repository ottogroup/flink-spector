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

import org.flinkspector.CoreSpec
import org.flinkspector.matcher.ListMatchers

class ListMatchersSpec extends CoreSpec {

  "the only matcher" should "succeed when handed two list with the same elements" in {
    ListMatchers
      .containsOnly[Integer](List(1, 2, 3, 4))
      .matches(List(1, 3, 2, 4)) shouldBe true
  }

  it should "fail if org.apache.flink.core.input" +
    " contains a element that is not expected" in {
    ListMatchers
        .containsOnly[Integer](List(1, 3, 2, 4))
      .matches(List(1, 2, 3, 4, 5)) shouldBe false

  }

  "the noDuplicates matcher" should "succeed when handed a list without duplicates" in {
    ListMatchers
      .containsNoDuplicates[Integer](List(1, 2, 3, 4))
      .matches(List(1, 2, 3, 4)) shouldBe true
  }

  it should "fail when handed duplicates" in {
    ListMatchers
       .containsNoDuplicates[Integer](List(3))
      .matches(List(1, 2, 3, 4, 3)) shouldBe false
  }

  it should "succeed when the same duplicates" in {
    ListMatchers
       .containsNoDuplicates[Integer](List(3,3))
      .matches(List(1, 2, 3, 4, 3, 4)) shouldBe true
  }

  it should "fail when handed more of the same elements" in {
    ListMatchers
       .containsNoDuplicates[Integer](List(3, 3, 4, 4))
      .matches(List(1, 2, 3, 4, 3, 3)) shouldBe false
  }

  "the inOrder matcher" should "succeed when handed elements in order" in {
    ListMatchers
      .containsInOrder[Integer](List(1, 3, 5))
      .matches(List(1, 2, 3, 4, 5)) shouldBe true
  }

  it should "fail when handed elements out of order" in {
    ListMatchers
        .containsInOrder[Integer](List(1, 2, 3))
      .matches(List(3, 2, 1)) shouldBe false
  }

  "the all matcher" should "succeed when all elements are present" in {
    ListMatchers
      .containsAll[Integer](List(2, 3, 4))
      .matches(List(1, 2, 3, 4)) shouldBe true
  }

  it should "fail when an elements is missing" in {
    ListMatchers
        .containsAll[Integer](List(2, 3, 5))
      .matches(List(1, 2, 3, 4)) shouldBe false
  }

  "the in series matcher" should "succeed when the lists are the same" in {
    ListMatchers
      .containsInSeries(List(1, 2, 3, 4))
      .matches(List(1, 2, 3, 4)) shouldBe true
  }

  it should "succeed when part of the lists are the same" in {
    ListMatchers
      .containsInSeries(List(2, 3, 4))
      .matches(List(1, 2, 3, 4)) shouldBe true
    ListMatchers
      .containsInSeries(List(2, 3))
      .matches(List(1, 2, 3, 4)) shouldBe true
    ListMatchers
      .containsInSeries(List(1, 2, 3))
      .matches(List(1, 2, 3, 4)) shouldBe true
  }

  it should "fail when part of the lists are not the same" in {
    ListMatchers
      .containsInSeries(List(2, 3, 4))
      .matches(List(1, 5, 3, 4)) shouldBe false
    ListMatchers
      .containsInSeries(List(2, 3))
      .matches(List(1, 1, 3, 4)) shouldBe false
    ListMatchers
      .containsInSeries(List(1, 2, 3))
      .matches(List(1, 2, 2, 4)) shouldBe false
  }


}
