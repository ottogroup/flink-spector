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

package org.flinkspector.dataset

import java.util
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.flink.api.common.functions.MapFunction
import org.flinkspector.core.input.InputBuilder
import org.flinkspector.core.runtime.{FlinkTestFailedException, SimpleOutputVerifier}
import org.flinkspector.core.trigger.VerifyFinishedTrigger
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConversions._

class DataSetEnvironmentSpec extends CoreSpec {

  class CountTrigger(n: Int) extends VerifyFinishedTrigger[Integer] {
    override def onRecord(record: Integer): Boolean = false

    override def onRecordCount(count: Long): Boolean = count >= n
  }


  class Verifier[T](list: List[T]) extends SimpleOutputVerifier[T] {

    override def verify(output: JList[T]): Unit =
      output should contain theSameElementsAs list
  }

  "The batch environment" should "initialize" in {
    DataSetTestEnvironment.createTestEnvironment(1)
  }

  it should "provide a DataStreamSource" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2, 3, 4, 5)))
    dataSet.output(outputFormat)
    env.executeTest()
  }

  it should "provide a DataStreamSource and throw an exception" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 3, 4, 5)))
    dataSet.output(outputFormat)
    an [TestFailedException] shouldBe thrownBy (env.executeTest())
  }

  it should "stop with trigger and signal a success" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2)), new CountTrigger(2))
    dataSet.output(outputFormat)
    env.executeTest()
  }

  it should "stop with trigger and signal a failure" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2, 3)), new CountTrigger(2))
    dataSet.output(outputFormat)
    an [TestFailedException] shouldBe thrownBy (env.executeTest())
  }

  it should "provide a DataStreamSource from [[Input]]" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val input = new InputBuilder[Int]().emitAll(List(1, 2, 3, 4, 5))
    val dataSet = env.createTestSet(input)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2, 3, 4, 5)))
    dataSet.output(outputFormat)
    env.executeTest()
  }

  it should "trigger a successful time-out" in {
    val happyVerifier = new SimpleOutputVerifier[Integer] {
      override def verify(output: util.List[Integer]): Unit = {}
    }

    val env = DataSetTestEnvironment.createTestEnvironment(1)
    env.setTimeoutInterval(1000)
    val list = List[Integer](1,2,3,4)
    val dataSet = env.fromElements(list: _*)
      .map(new MapFunction[Integer,Integer] {
        override def map(value: Integer): Integer = {
          val start = System.currentTimeMillis()
          while(System.currentTimeMillis() - start < 1000L){
            Thread.sleep(100)
          }
          value + 1
        }
      })
    val outputFormat = env.createTestOutputFormat(happyVerifier)
    dataSet.output(outputFormat)
    env.executeTest()

    //check if a forceful stop was invoked
    env.hasBeenStopped shouldBe true

  }

  it should "trigger a failed time-out" in {
    val sadVerifier = new SimpleOutputVerifier[Integer] {
      override def verify(output: util.List[Integer]): Unit = {
        throw new FlinkTestFailedException(new AssertionError())
      }
    }

    val env = DataSetTestEnvironment.createTestEnvironment(1)
    env.setTimeoutInterval(1000)
    val list = List[Integer](1,2,3,4)
    val dataSet = env.fromElements(list: _*)
      .map(new MapFunction[Integer, Integer] {
        override def map(t: Integer): Integer = {
          val start = System.currentTimeMillis()
          while(System.currentTimeMillis() - start < 1000L){}
          t + 1
        }
      })
    val outputFormat = env.createTestOutputFormat(sadVerifier)
    dataSet.output(outputFormat)
//    failAfter(Span(10,Seconds)) {
      an [FlinkTestFailedException] shouldBe thrownBy (env.executeTest())
//    }
    //check if a forceful stop was invoked
    env.hasBeenStopped shouldBe true

  }

  it should "handle more than one outputFormat" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2,4,6,8)
    val oddlist = List[Integer](1,3,5,7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)

    val evenOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(2, 4, 6, 8)))
    val oddOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(1, 3, 5, 7)))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    env.executeTest()

  }

  it should "throw an exception if a verifier failed" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2, 3, 4)))
    dataSet.output(outputFormat)
    an [TestFailedException] shouldBe thrownBy(env.executeTest())

  }

  it should "handle one failure with multiple outputFormats" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2,4,6,8)
    val oddlist = List[Integer](1,3,5,7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)

    val evenOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(2, 4, 6, 8)))
    val oddOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(2, 3, 5, 7)))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    an[TestFailedException] shouldBe thrownBy(env.executeTest())

  }


  it should "handle more than one failures with multiple outputFormats" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2,4,6,8)
    val oddlist = List[Integer](1,3,5,7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)

    val evenOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(1, 4, 6, 8)))
    val oddOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(2, 3, 5, 7)))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    //TODO shutdown at failure
    an [TestFailedException] shouldBe thrownBy(env.executeTest())
  }

  it should "not stop if only one trigger fires with multiple outputFormats" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    env.setTimeoutInterval(10000)
    val evenlist = List[Integer](2,4,6,8)
    val oddlist = List[Integer](1,3,5,7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)

    val evenOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(2, 4)), new CountTrigger(2))
    val oddOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(1, 3, 5, 7)))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    env.executeTest()
  }

  it should "stop if all triggers fire" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2,4,6,8)
    val oddlist = List[Integer](1,3,5,7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)


    val evenOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(2, 4)), new CountTrigger(2))
    val oddOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(1, 3)), new CountTrigger(2))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    env.executeTest()
    //check for flag

  }


}
