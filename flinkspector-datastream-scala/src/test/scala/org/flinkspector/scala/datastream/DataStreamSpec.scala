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
import org.flinkspector.core.quantify.HamcrestVerifier

case class Output(key: String, value: Int)

class DataStreamTest extends CoreSpec {

  "basic test" should "work" in {

    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val stream = env.fromCollection(List(1, 2, 3, 4)).map(_ + 1)

    val expected = ExpectedRecords
      .create(2)
      .expect(3)
      .expect(4)
      .expect(5)
    
    val verifier = new HamcrestVerifier[Int](expected)

    val sink = env.createTestSink(verifier)

    stream.addSink(sink)

    env.executeTest()
  }

  "assert test" should "work" in {

    /**
     * Use a case class as a mask
     */
    val block = new AssertBlock[Output]{
      assertThat(v.key shouldBe a[String])
      assertThat(v.value should be > 5)
    }

    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val input = InputBuilder.startWith(("test", 9))
      .emit(("check", 5))
      .emit(("check", 6))
      .emit(("check", 15))
      .emit(("check", 23))

    val stream = env.fromInput(input)

    val verifier = new HamcrestVerifier[(String, Int)](block)

    val sink = env.createTestSink(verifier)

    stream.addSink(sink)

    an [AssertionError] shouldBe thrownBy (env.executeTest())
  }
}


