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
package org.flinkspector.core.quantify.assertions

import org.apache.flink.api.java.tuple.{Tuple3 => Fluple3}
import org.flinkspector.core.quantify.TupleMask
import org.flinkspector.core.{CoreSpec, KeyMatcherPair}
import org.hamcrest.{Matcher, Matchers}

import scala.collection.JavaConverters._

class TupleMapMatchersSpec extends CoreSpec {

  trait TupleMapMatchersCase {
    val mask = new TupleMask[Fluple3[Int, Int, Int]]("one", "two", "three")
    val matcherList = List(
      new TupleMatcher[Fluple3[Int,Int,Int]](KeyMatcherPair.of("one", Matchers.is(1)),mask) : Matcher[_ >: Fluple3[Int,Int,Int]],
      new TupleMatcher[Fluple3[Int,Int,Int]](KeyMatcherPair.of("two", Matchers.is(1)),mask): Matcher[_ >: Fluple3[Int,Int,Int]],
      new TupleMatcher[Fluple3[Int,Int,Int]](KeyMatcherPair.of("three", Matchers.is(1)),mask): Matcher[_ >: Fluple3[Int,Int,Int]])
  }

  "The any matcher" should "implement any" in new TupleMapMatchersCase {
    val matcher = Any.any[Fluple3[Int,Int,Int]](matcherList.asJava)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe true
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe true
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false
  }

  "The each matcher" should "implement each" in new TupleMapMatchersCase {
    val matcher = Each.each[Fluple3[Int,Int,Int]](matcherList.asJava)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe true
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe false
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe false
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false

  }

  "The one matcher" should "implement one" in new TupleMapMatchersCase {
    val matcher = One.one[Fluple3[Int,Int,Int]](matcherList.asJava)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe false
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe false
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe true
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false
  }

  "The exactly matcher" should "implement exactly" in new TupleMapMatchersCase {
    val matcher = Exactly.exactly[Fluple3[Int,Int,Int]](matcherList.asJava, 2)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe false
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe false
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false

  }

  "The atMost matcher" should "implement atMost" in new TupleMapMatchersCase {
    val matcher = AtMost.atMost[Fluple3[Int,Int,Int]](matcherList.asJava, 2)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe false
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe true
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe true

  }
  
  "The atLeast matcher" should "implement atLeast" in new TupleMapMatchersCase {
    val matcher = AtLeast.atLeast[Fluple3[Int,Int,Int]](matcherList.asJava, 2)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe true
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe false
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false

  }

}
