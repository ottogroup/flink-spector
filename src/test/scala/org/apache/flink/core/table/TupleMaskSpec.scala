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
import org.apache.flink.streaming.test.CoreSpec

class TupleMaskSpec extends CoreSpec {

  "The mask" should "map keys to a tuple" in {
    val mask = new TupleMask[Fluple2[Int, Int]]("key", "value")
    val tmap = mask.apply(Fluple2.of(1, 2))
    tmap.get[Int]("key") shouldBe 1
    tmap.get[Int]("value") shouldBe 2
  }

  it should "map keys with null as placeholder" in {
    val mask = new TupleMask[Fluple3[Int,Int,Int]](null,null,"value")
    val tmap = mask.apply(Fluple3.of(1,2,3))
    tmap.get[Int]("value") shouldBe 3
  }
}
