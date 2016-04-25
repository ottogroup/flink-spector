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
package org.flinkspector.core.quantify.records

import org.flinkspector.core.CoreSpec
import org.hamcrest.Matchers

import scala.collection.JavaConverters._

class QuantifyMatchersSpec extends CoreSpec {

  //TODO make this test better readable
  val list = List(1,2,3,4,5,6,7,8)

  "The OnAny matcher" should "implement any" in {
    OnAny.any(Matchers.is(2))
      .matches(list.asJava) should be(true)
    OnAny.any(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(true)
    OnAny.any(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(true)
    OnAny.any(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The OnEach matcher" should "implement each" in {
    OnEach.each(Matchers.is(2))
      .matches(list.asJava) should be(false)
    OnEach.each(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    OnEach.each(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(true)
    OnEach.each(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The OnOne matcher" should "implement one" in {
    OnOne.one(Matchers.is(2))
      .matches(list.asJava) should be(true)
    OnOne.one(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    OnOne.one(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(false)
    OnOne.one(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The OnAtLeast matcher" should "implement atLeast" in {
    OnAtLeast.atLeast(Matchers.is(2),2)
      .matches(list.asJava) should be(false)
    OnAtLeast.atLeast(Matchers.greaterThan(new Integer(6)),2)
      .matches(list.asJava) should be(true)
    OnAtLeast.atLeast(Matchers.lessThan(new Integer(12)),2)
      .matches(list.asJava) should be(true)
    OnAtLeast.atLeast(Matchers.greaterThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
  }

  "The OnExactly matcher" should "implement atMost" in {
    OnExactly.exactly(Matchers.is(2),2)
      .matches(list.asJava) should be(false)
    OnExactly.exactly(Matchers.greaterThan(new Integer(6)),2)
      .matches(list.asJava) should be(true)
    OnExactly.exactly(Matchers.lessThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
    OnExactly.exactly(Matchers.greaterThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
  }

  "The None matcher" should "implement none" in {
    ListQuantifyMatchers.none(Matchers.is(2))
      .matches(list.asJava) should be(false)
    ListQuantifyMatchers.none(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    ListQuantifyMatchers.none(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(false)
    ListQuantifyMatchers.none(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(true)
  }


}
