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

package org.flinkspector.datastream

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.flinkspector.CoreSpec
import org.flinkspector.core.input.InputBuilder
import org.flinkspector.core.runtime.SimpleOutputVerifier
import org.flinkspector.core.trigger.VerifyFinishedTrigger
import org.flinkspector.datastream.input.EventTimeInputBuilder
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConversions._

class StreamTestEnvironmentSpec extends CoreSpec {

  class Verifier[T](list: List[T]) extends SimpleOutputVerifier[T] {
    override def verify(output: JList[T]): Unit =
      output should contain theSameElementsAs list
  }

  class CountTrigger(n: Int) extends VerifyFinishedTrigger[Integer] {
    override def onRecord(record: Integer): Boolean = false

    override def onRecordCount(count: Long): Boolean = count >= n
  }

  class CountVerifier[T](cnt: Int) extends SimpleOutputVerifier[T] {
    override def verify(output: JList[T]): Unit =
      output should have length(cnt)
  }

  "The stream environment" should "initialize" in {
    DataStreamTestEnvironment.createTestEnvironment(1)
  }

  it should "provide a DataStreamSource" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val source = env.fromElements(1, 2, 3, 4, 5)
    val sink = env.createTestSink(new Verifier(List(1, 2, 3, 4, 5)))
    source.addSink(sink)
    env.executeTest()
  }

  it should "stop with trigger and signal a success" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val source = env.fromElements(1, 2, 3, 4, 5)
    val sink = env.createTestSink(new CountVerifier[Int](2), new CountTrigger(2))
    source.addSink(sink)
    env.executeTest()
  }

  it should "stop with trigger and signal a failure" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val source = env.fromElements(1, 2, 3, 4, 5)
    val sink = env.createTestSink(new Verifier(List(1, 2, 3)), new CountTrigger(2))
    source.addSink(sink)
    an[TestFailedException] shouldBe thrownBy(env.executeTest())
  }

  it should "provide a DataStreamSource from [[Input]]" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val input = new InputBuilder[Int]().emitAll(List(1, 2, 3, 4, 5))
    val source = env.fromInput(input)
    val sink = env.createTestSink(new Verifier(List(1, 2, 3, 4, 5)))
    source.addSink(sink)
    env.executeTest()
  }

  it should "provide a time-stamped DataStreamSource" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val input = EventTimeInputBuilder.startWith[Int](new StreamRecord[Int](1, 1))
      .emit(new StreamRecord[Int](2, 2))
      .emit(new StreamRecord[Int](3, 3))
      .emit(new StreamRecord[Int](4, 4))
    val source = env.fromInput(input)
    val sink = env.createTestSink(new Verifier(List(1, 2, 3, 4)))
    source.addSink(sink)
    env.executeTest()
  }

  it should "handle more than one sink" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenStream = env.fromElements(evenlist: _*)
    val oddStream = env.fromElements(oddlist: _*)


    val evenSink = env.createTestSink(new Verifier[Integer](List(2, 4, 6, 8)))
    val oddSink = env.createTestSink(new Verifier[Integer](List(1, 3, 5, 7)))
    evenStream.addSink(evenSink)
    oddStream.addSink(oddSink)
    env.executeTest()

  }

  it should "throw an exception if a verifier failed" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val source = env.fromElements(1, 2, 3, 4, 5)
    val sink = env.createTestSink(new Verifier(List(1, 2, 3, 4)))
    source.addSink(sink)
    an[TestFailedException] shouldBe thrownBy(env.executeTest())

  }

  it should "handle one failure with multiple sinks" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenStream = env.fromElements(evenlist: _*)
    val oddStream = env.fromElements(oddlist: _*)

    val evenSink = env.createTestSink(new Verifier[Integer](List(2, 4, 6, 8)))
    val oddSink = env.createTestSink(new Verifier[Integer](List(2, 3, 5, 7)))
    evenStream.addSink(evenSink)
    oddStream.addSink(oddSink)
    an[TestFailedException] shouldBe thrownBy(env.executeTest())

  }


  it should "handle more than one failures with multiple sinks" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenStream = env.fromElements(evenlist: _*)
    val oddStream = env.fromElements(oddlist: _*)

    val evenSink = env.createTestSink(new Verifier[Integer](List(1, 4, 6, 8)))
    val oddSink = env.createTestSink(new Verifier[Integer](List(2, 3, 5, 7)))
    evenStream.addSink(evenSink)
    oddStream.addSink(oddSink)
    //TODO shutdown at failure
    an[TestFailedException] shouldBe thrownBy(env.executeTest())
  }

  it should "not stop if only one trigger fires with multiple sinks" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    env.setTimeoutInterval(10000)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenStream = env.fromElements(evenlist: _*)
    val oddStream = env.fromElements(oddlist: _*)

    val evenSink = env.createTestSink(new CountVerifier[Integer](2), new CountTrigger(2))
    val oddSink = env.createTestSink(new Verifier[Integer](List(1, 3, 5, 7)))
    evenStream.addSink(evenSink)
    oddStream.addSink(oddSink)
    env.executeTest()

    //    testEnv.hasBeenStopped shouldBe false
  }

  it should "stop if all triggers fire" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenStream = env.fromElements(evenlist: _*)
    val oddStream = env.fromElements(oddlist: _*)


    val evenSink = env.createTestSink(new CountVerifier[Integer](2), new CountTrigger(2))
    val oddSink = env.createTestSink(new CountVerifier[Integer](2), new CountTrigger(2))
    evenStream.addSink(evenSink)
    oddStream.addSink(oddSink)
    env.executeTest()
    //check for flag

  }


}
