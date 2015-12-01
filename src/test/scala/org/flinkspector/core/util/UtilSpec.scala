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
package org.flinkspector.core.util

import java.util

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.flinkspector.CoreSpec
import org.flinkspector.util.Util

import scala.collection.JavaConversions._

class UtilSpec extends CoreSpec {

  "insertWatermarks" should "produce a list of watermarks" in {
    val list: List[Long] = List(3, 1, 11, 2, 5, 4, 10, 8, 7, 9)
    Util.produceWatermarks(list) shouldBe List(-1, 1, -1, 3, -1, 5, -1, -1, 8, 11)
  }

  "insertWatermarks" should "produce a list of sorted watermarks" in {
    val list: List[Long] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    Util.produceWatermarks(list) shouldBe List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  }

  "insertWatermarks" should "produce one watermark" in {
    val list: List[Long] = List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
    Util.produceWatermarks(list) shouldBe List(-1, -1, -1, -1, -1, -1, -1, -1, -1, 10)
  }

  "calculateWatermarks" should "convert a list of streamrecords" in {
    val list = new util.ArrayList[StreamRecord[Integer]]()
    list.add(new StreamRecord[Integer](1, 2))
    val result = Util.calculateWatermarks(list)
    result should contain only (2)
  }

  "splitList" should "split the input correctly in 1 partition" in {
    Util.splitList(List(1, 2, 3, 4, 5, 6, 7, 8), 0, 1) should contain only(1, 2, 3, 4, 5, 6, 7, 8)
  }

  "slitList" should "split the input correctly in 2 partitions" in {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    Util.splitList(list, 0, 2) should contain only(1, 3, 5, 7)
    Util.splitList(list, 1, 2) should contain only(2, 4, 6, 8)
  }


  "slitList" should "split the input correctly in 3 partitions" in {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    Util.splitList(list, 0, 3) should contain only(1, 4, 7)
    Util.splitList(list, 1, 3) should contain only(2, 5, 8)
    Util.splitList(list, 2, 3) should contain only(3, 6)
  }

  "slitList" should "split the input correctly in 4 partitions" in {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    Util.splitList(list, 0, 4) should contain only(1, 5)
    Util.splitList(list, 1, 4) should contain only(2, 6)
    Util.splitList(list, 2, 4) should contain only(3, 7)
    Util.splitList(list, 3, 4) should contain only(4, 8)
  }

  "prepend" should "prepend an element to an array" in {
    val array = Array("2", "3", "4", "5")
    Util.prepend("1", array) shouldBe Array("1", "2", "3", "4", "5")
  }

}
