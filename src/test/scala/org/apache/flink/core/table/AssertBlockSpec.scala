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

package org.apache.flink.core.table

import org.apache.flink.api.java.tuple.{Tuple3 => Fluple3, Tuple4 => Fluple4}
import org.apache.flink.streaming.test.CoreSpec
import org.hamcrest.core.Is

import scala.collection.JavaConversions._

class AssertBlockSpec extends CoreSpec {

  "The AssertBlock" should "store a list of [[KeyMatcherPair]]s" in {
    val matcher = Is.is(1)
    val block = new AssertBlock[Fluple3[Int, Int, Int]]("1", "2", "3")
    block.assertThat("1", matcher)
    block.assertThat("2", matcher)
    block.assertThat("2", matcher)

    val pairs = block.getKeyMatcherPairs
    pairs.get(0).key shouldBe "1"
    pairs.get(0).matcher shouldBe matcher
    pairs.get(1).key shouldBe "2"
    pairs.get(2).key shouldBe "2"
    pairs.get(1).matcher shouldBe matcher
    pairs.get(2).matcher shouldBe matcher
  }

  it should "use each per default" in new AssertBlockCase {
    block.onEachRecord().matchesSafely(mkFluple(1, 1, 1, 1)) shouldBe true
    block.onEachRecord().matchesSafely(mkFluple(1, 0, 1, 1)) shouldBe false
  }

  it should "provide anyOf" in new AssertBlockCase {
    block.anyOfThem().onEachRecord()
      .matchesSafely(mkFluple(1, 1, 1, 1)) shouldBe true
    block.anyOfThem().onEachRecord()
      .matchesSafely(mkFluple(1, 1, 0, 1)) shouldBe true
    block.anyOfThem().onEachRecord()
      .matchesSafely(mkFluple(0, 0, 0, 0)) shouldBe false
  }

  it should "provide oneOf" in new AssertBlockCase {
    block.oneOfThem().onEachRecord()
      .matchesSafely(mkFluple(1, 1, 1, 1)) shouldBe false
    block.oneOfThem().onEachRecord()
      .matchesSafely(mkFluple(1, 0, 0, 0)) shouldBe true
    block.oneOfThem().onEachRecord()
      .matchesSafely(mkFluple(0, 0, 0, 0)) shouldBe false
  }

  it should "provide atLeastOf" in new AssertBlockCase {
    block.atLeastNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(1, 0, 1, 1)) shouldBe true
    block.atLeastNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(1, 0, 0, 1)) shouldBe true
    block.atLeastNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(0, 1, 0, 0)) shouldBe false
  }

  it should "provide atMostOf" in new AssertBlockCase {
    block.atMostNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(1, 0, 1, 1)) shouldBe false
    block.atMostNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(1, 0, 0, 1)) shouldBe true
    block.atMostNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(0, 0, 0, 1)) shouldBe true
  }

  it should "provide exactlyOf" in new AssertBlockCase {
    block.exactlyNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(1, 1, 1, 1)) shouldBe false
    block.exactlyNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(1, 1, 0, 0)) shouldBe true
    block.exactlyNOfThem(2).onEachRecord()
      .matchesSafely(mkFluple(0, 0, 0, 1)) shouldBe false
  }

  trait AssertBlockCase {
    val matcher = Is.is(1)
    val block =
      new AssertBlock[Fluple4[Int, Int, Int, Int]]("1", "2", "3", "4")
    block.assertThat("1", matcher)
    block.assertThat("2", matcher)
    block.assertThat("3", matcher)
    block.assertThat("4", matcher)
  }

  def mkFluple(v1: Int, v2: Int, v3: Int, v4: Int)
  : List[Fluple4[Int, Int, Int, Int]] = {
    List(Fluple4.of(v1, v2, v3, v4))
  }
}
