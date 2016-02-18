package org.flinkspector.scala.datastream

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

import org.apache.flink.api.scala._
import org.flinkspector.core.collection.ExpectedRecords
import org.flinkspector.core.input.InputBuilder

//needs to be defined top level
case class Output(key: String, value: Int)

class DataStreamSpec extends CoreSpec with FlinkDataStream{

  "basic test" should "work" in {

    //create a test stream
    val stream = createTestStream(List(1, 2, 3, 4)).map(_ + 1)

    //build a matcher
    val expected = ExpectedRecords
      .create(2)
      .expect(3)
      .expect(4)
      .expect(5)

    //use the matcher on the datastream
    stream should fulfill(expected)
    executeTest()

  }


  "assert test" should "work" in {

    val input = InputBuilder.startWith(("test", 9))
      .emit(("check", 5))
      .emit(("check", 6))
      .emit(("check", 15))
      .emit(("check", 23))

    val stream = createTestStream(input)

    stream should fulfill {
      //use a case class to map the tuple
      new AssertBlock[(String,Int),Output] {
        field(v.key shouldBe a[String])
        field(v.value should be > 4)
      }
    }
    executeTest()
  }
}


