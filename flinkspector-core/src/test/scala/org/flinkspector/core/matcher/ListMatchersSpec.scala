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

  it should "fail when an elements is has a lower frequency" in {
    ListMatchers
      .containsOnly[Integer](List(2, 2, 3, 4))
      .matches(List(2, 3, 4)) shouldBe false
  }

  it should "succeed when an elements is has same frequency" in {
    ListMatchers
      .containsOnly[Integer](List(2, 2, 3, 4))
      .matches(List(2, 2, 3, 4)) shouldBe true
  }

  it should "succeed when an elements is has higher frequency" in {
    ListMatchers
      .containsOnly[Integer](List(2, 2, 3, 4))
      .matches(List(2, 2, 2, 3, 4)) shouldBe true
  }

  it should "succeed with nested structure" in {
    ListMatchers
      .containsOnly[Map[String, Integer]](List(Map("k" -> 2), Map("k" -> 3)))
      .matches(List(Map("k" -> 2), Map("k" -> 3))) shouldBe true
  }

  it should "succeed with nested structure and duplicates" in {
    ListMatchers
      .containsOnly[Map[String, Integer]](List(Map("k" -> 2), Map("k" -> 3), Map("k" -> 3)))
      .matches(List(Map("k" -> 2), Map("k" -> 3), Map("k" -> 3))) shouldBe true
  }

  it should "fail with nested structure and duplicates" in {
    ListMatchers
      .containsOnly[Map[String, Integer]](List(Map("k" -> 2), Map("k" -> 3), Map("k" -> 3)))
      .matches(List(Map("k" -> 2), Map("k" -> 3))) shouldBe false
  }

  it should "succeed with nested lists" in {
    ListMatchers
      .containsOnly[List[String]](List(List("k", "2"), List("k", "3")))
      .matches(List(List("k", "2"), List("k", "3"))) shouldBe true
  }

  it should "succeed with nested lists and duplicates" in {
    ListMatchers
      .containsOnly[List[String]](List(List("k", "2"), List("k", "3"), List("k", "3")))
      .matches(List(List("k", "2"), List("k", "3"), List("k", "3"))) shouldBe true
  }

  it should "fail with nested lists and duplicates" in {
    ListMatchers
      .containsOnly[List[String]](List(List("k", "2"), List("k", "3"), List("k", "3")))
      .matches(List(List("k", "2"), List("k", "3"))) shouldBe false
  }

  "the sameFrequency matcher" should "succeed when handed a list without duplicates" in {
    ListMatchers
      .sameFrequency[Integer](List(1, 2, 3, 4))
      .matches(List(1, 2, 3, 4)) shouldBe true
  }

  it should "fail when handed duplicates" in {
    ListMatchers
      .sameFrequency[Integer](List(3))
      .matches(List(1, 2, 3, 4, 3)) shouldBe false
  }

  it should "succeed when the same duplicates" in {
    ListMatchers
      .sameFrequency[Integer](List(3, 3))
      .matches(List(1, 2, 3, 4, 3, 4)) shouldBe true
  }

  it should "fail when handed more of the same elements" in {
    ListMatchers
      .sameFrequency[Integer](List(3, 3, 4, 4))
      .matches(List(1, 2, 3, 4, 3, 3)) shouldBe false
  }

  it should "fail when handed less of the same elements" in {
    ListMatchers
      .sameFrequency[Integer](List(3, 3, 4, 4))
      .matches(List(1, 2, 3, 4, 4)) shouldBe false
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

  it should "fail when an elements is has a lower frequency" in {
    ListMatchers
      .containsAll[Integer](List(2, 2, 3, 4))
      .matches(List(1, 2, 3, 4)) shouldBe false
  }

  it should "succeed when an elements is has same frequency" in {
    ListMatchers
      .containsAll[Integer](List(2, 2, 3, 4))
      .matches(List(1, 2, 2, 3, 4)) shouldBe true
  }

  it should "succeed when an elements is has higher frequency" in {
    ListMatchers
      .containsAll[Integer](List(2, 2, 3, 4))
      .matches(List(2, 1, 2, 2, 3, 4)) shouldBe true
  }

  it should "succeed with nested structure" in {
    ListMatchers
      .containsAll[Map[String, Integer]](List(Map("k" -> 2), Map("k" -> 3)))
      .matches(List(Map("k" -> 2), Map("k" -> 3))) shouldBe true
  }

  it should "succeed with nested structure and duplicates" in {
    ListMatchers
      .containsAll[Map[String, Integer]](List(Map("k" -> 2), Map("k" -> 3), Map("k" -> 3)))
      .matches(List(Map("k" -> 2), Map("k" -> 3), Map("k" -> 3))) shouldBe true
  }

  it should "fail with nested structure and duplicates" in {
    ListMatchers
      .containsAll[Map[String, Integer]](List(Map("k" -> 2), Map("k" -> 3), Map("k" -> 3)))
      .matches(List(Map("k" -> 2), Map("k" -> 3))) shouldBe false
  }

  it should "succeed with nested lists" in {
    ListMatchers
      .containsAll[List[String]](List(List("k", "2"), List("k", "3")))
      .matches(List(List("k", "2"), List("k", "3"))) shouldBe true
  }

  it should "succeed with nested lists and duplicates" in {
    ListMatchers
      .containsAll[List[String]](List(List("k", "2"), List("k", "3"), List("k", "3")))
      .matches(List(List("k", "2"), List("k", "3"), List("k", "3"))) shouldBe true
  }

  it should "fail with nested lists and duplicates" in {
    ListMatchers
      .containsAll[List[String]](List(List("k", "2"), List("k", "3"), List("k", "3")))
      .matches(List(List("k", "2"), List("k", "3"))) shouldBe false
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
