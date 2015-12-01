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

package org.flinkspector.core.table

import org.flinkspector.CoreSpec
import org.hamcrest.core.Is

import scala.collection.JavaConversions._

class ResultMatcherSpec extends CoreSpec {

  "The ResultMatcher" should "provide eachOf" in {
    val matcher = Is.is(1)
    val resultMatcher = new ResultMatcher[Int](matcher)

    resultMatcher.onEachRecord().matchesSafely(List(1, 1, 1, 1)) shouldBe true
    resultMatcher.onEachRecord().matchesSafely(List(1, 1, 0, 1)) shouldBe false
  }

  it should "provide anyOf" in {
    val matcher = Is.is(1)
    val resultMatcher = new ResultMatcher[Int](matcher)

    resultMatcher.onAnyRecord().matchesSafely(List(1, 1, 1, 1)) shouldBe true
    resultMatcher.onAnyRecord().matchesSafely(List(1, 1, 0, 1)) shouldBe true
    resultMatcher.onAnyRecord().matchesSafely(List(0, 0, 0, 0)) shouldBe false
  }

  it should "provide noneOf" in {
    val matcher = Is.is(1)
    val resultMatcher = new ResultMatcher[Int](matcher)

    resultMatcher.onNoRecord().matchesSafely(List(1, 0, 1, 1)) shouldBe false
    resultMatcher.onNoRecord().matchesSafely(List(0, 0, 0, 0)) shouldBe true
  }

  it should "provide oneOf" in {
    val matcher = Is.is(1)
    val resultMatcher = new ResultMatcher[Int](matcher)

    resultMatcher.onOneRecord().matchesSafely(List(1, 0, 1, 1)) shouldBe false
    resultMatcher.onOneRecord().matchesSafely(List(0, 1, 0, 0)) shouldBe true
  }

  it should "provide atLeast" in {
    val matcher = Is.is(1)
    val resultMatcher = new ResultMatcher[Int](matcher)

    resultMatcher.onAtLeastNRecords(2).matchesSafely(List(1, 0, 1, 1)) shouldBe true
    resultMatcher.onAtLeastNRecords(2).matchesSafely(List(0, 1, 0, 0)) shouldBe false
    resultMatcher.onAtLeastNRecords(2).matchesSafely(List(1, 1, 0, 0)) shouldBe true
  }

  it should "provide atMost" in {
    val matcher = Is.is(1)
    val resultMatcher = new ResultMatcher[Int](matcher)

    resultMatcher.onAtMostNRecords(2).matchesSafely(List(1, 0, 1, 1)) shouldBe false
    resultMatcher.onAtMostNRecords(2).matchesSafely(List(0, 1, 0, 0)) shouldBe true
    resultMatcher.onAtMostNRecords(2).matchesSafely(List(1, 1, 0, 0)) shouldBe true
  }

  it should "provide exactly" in {
    val matcher = Is.is(1)
    val resultMatcher = new ResultMatcher[Int](matcher)

    resultMatcher.onExactlyNRecords(2).matchesSafely(List(1, 0, 1, 1)) shouldBe false
    resultMatcher.onExactlyNRecords(2).matchesSafely(List(0, 1, 0, 0)) shouldBe false
    resultMatcher.onExactlyNRecords(2).matchesSafely(List(1, 1, 0, 0)) shouldBe true
  }
}
