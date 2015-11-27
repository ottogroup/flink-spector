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

package org.apache.flink.core.set

import org.apache.flink.streaming.test.CoreSpec

import scala.collection.JavaConversions._

class MatcherBuilderSpec extends CoreSpec {

  "The MatcherBuilder" should "check for all per default" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    builder.matchesSafely(List(1, 2, 3)) shouldBe false
    builder.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe true
  }

  it should "check for all if only was not defined" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.noDuplicates()

    builder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    builder.matchesSafely(List(1, 2, 3)) shouldBe false
    builder.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe true
  }

  it should "check for only if only was defined" in {
    val onlyBuilder = new MatcherBuilder[Int](List(1, 2, 3, 4)).only()

    onlyBuilder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    onlyBuilder.matchesSafely(List(1, 2, 3)) shouldBe false
    onlyBuilder.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe false
  }

  it should "check for only if only was defined in combination" in {
    val onlyBuilder = new MatcherBuilder[Int](List(1, 2, 3, 4))
      .only()
      .noDuplicates()

    onlyBuilder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    onlyBuilder.matchesSafely(List(1, 2, 3)) shouldBe false
    onlyBuilder.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe false
  }

  it should "check for order" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.inOrder(MatcherBuilder.Order.NONSTRICT).all()

    builder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    builder.matchesSafely(List(1, 2, 4, 3)) shouldBe false
  }

  it should "check for partial order" in {
    val fromToBuilder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    fromToBuilder.inOrder(MatcherBuilder.Order.NONSTRICT).from(1).to(2)

    fromToBuilder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    fromToBuilder.matchesSafely(List(1, 3, 2, 4)) shouldBe false

    val indicesBuilder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    indicesBuilder.inOrder(MatcherBuilder.Order.NONSTRICT).indices(0, 3)

    indicesBuilder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    indicesBuilder.matchesSafely(List(4, 3, 2, 1)) shouldBe false
  }

  it should "check for order in combination" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.only().inOrder(MatcherBuilder.Order.NONSTRICT).all()

    builder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    builder.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe false
    builder.matchesSafely(List(1, 2, 4, 3)) shouldBe false
  }

  it should "check for series" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.inOrder(MatcherBuilder.Order.STRICT).all()

    builder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    builder.matchesSafely(List(1, 2, 4, 3)) shouldBe false
  }

  it should "check for partial series" in {
    val fromToBuilder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    fromToBuilder.inOrder(MatcherBuilder.Order.STRICT).from(1).to(2)

    fromToBuilder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    fromToBuilder.matchesSafely(List(1, 3, 2, 4)) shouldBe false

    val indicesBuilder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    indicesBuilder.inOrder(MatcherBuilder.Order.STRICT).indices(0, 3)

    indicesBuilder.matchesSafely(List(1, 4, 2, 3)) shouldBe true
    indicesBuilder.matchesSafely(List(4, 2, 3, 1)) shouldBe false
  }

  it should "check for series in combination" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.only().inOrder(MatcherBuilder.Order.STRICT).all()

    builder.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    builder.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe false
    builder.matchesSafely(List(1, 2, 4, 3)) shouldBe false
  }
  it should "check for duplicates" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.noDuplicates()

    builder.matchesSafely(List(1,2,3,4,5)) shouldBe true
    builder.matchesSafely(List(1,2,3,4,4)) shouldBe false
  }

  it should "check for duplicates in combination" in {
    val builder = new MatcherBuilder[Int](List(1, 2, 3, 4))
    builder.noDuplicates().only()

    builder.matchesSafely(List(1,2,3,4)) shouldBe true
    builder.matchesSafely(List(1,2,3,4,5)) shouldBe false
    builder.matchesSafely(List(1,2,3,4,4)) shouldBe false
  }


}
