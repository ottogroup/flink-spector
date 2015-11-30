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

import org.apache.flink.api.java.tuple.{Tuple2 => Fluple2, Tuple3 => Fluple3}
import org.apache.flink.streaming.CoreSpec

class TupleMapSpec extends CoreSpec {

  "The tuple map" should "map a tuple" in {
    val tmap = new TupleMap[Fluple2[String, Int]](
      Fluple2.of("test", 2), Array("key", "value")
    )

    tmap.getKeys should contain only("key", "value")
    tmap.get[String]("key") shouldBe "test"
    tmap.get[Int]("value") shouldBe 2
  }

  it should "partially map a tuple" in {
    val tmap = new TupleMap[Fluple2[String, Int]](
      Fluple2.of("test", 2), Array("key")
    )

    tmap.get[String]("key") shouldBe "test"
    an [IllegalArgumentException] shouldBe thrownBy (tmap.get[Integer]("value"))
  }

  it should "map a tuple with placeholder" in {
    val tmap = new TupleMap[Fluple2[String, Int]](
      Fluple2.of("test", 2), Array(null, "value")
    )

    tmap.get[Integer]("value") shouldBe 2

    an [IllegalArgumentException] shouldBe thrownBy (tmap.get[String](null))
  }

  it should "accept multiple placeholders" in {
    var tmap = new TupleMap[Fluple3[String, Int, Int]](
      Fluple3.of("test", 1, 2), Array(null, "value", null)
    )

    tmap.get[Int]("value") shouldBe 1

    tmap = new TupleMap[Fluple3[String, Int, Int]](
      Fluple3.of("test", 1, 2), Array(null, null, "value")
    )

    tmap.get[Int]("value") shouldBe 2
  }

  it should "not accept too many keys" in {
    an [IllegalArgumentException] shouldBe thrownBy {
      new TupleMap[Fluple3[String, Int, Int]](
        Fluple3.of("test", 1, 2), Array("key", "value", null, "v2")
      )
    }
  }
}
