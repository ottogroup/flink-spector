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

package io.flinkspector.dataset

import java.util.{ArrayList => JArrayList, List => JList}

import io.flinkspector.core.input.InputBuilder
import io.flinkspector.core.runtime.SimpleOutputVerifier
import io.flinkspector.core.trigger.VerifyFinishedTrigger
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

  class CountVerifier[T](cnt: Int) extends SimpleOutputVerifier[T] {

    override def verify(output: JList[T]): Unit =
      output should have length (cnt)
  }

  "The batch environment" should "initialize" in {
    DataSetTestEnvironment.createTestEnvironment(1)
  }

  it should "provide a OutputFormat" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2, 3, 4, 5)))
    dataSet.output(outputFormat)
    env.executeTest()
  }

  it should "provide a Outputformat and throw an exception" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 3, 4, 5)))
    dataSet.output(outputFormat)
    an[TestFailedException] shouldBe thrownBy(env.executeTest())
  }

  it should "stop with trigger and signal a success" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new CountVerifier[Int](2), new CountTrigger(2))
    dataSet.output(outputFormat)
    env.executeTest()
  }

  it should "stop with trigger and signal a failure" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val dataSet = env.fromElements(1, 2, 3, 4, 5)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2, 3)), new CountTrigger(2))
    dataSet.output(outputFormat)
    an[TestFailedException] shouldBe thrownBy(env.executeTest())
  }

  it should "provide a OutputFormat from [[Input]]" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val input = new InputBuilder[Int]().emitAll(List(1, 2, 3, 4, 5))
    val dataSet = env.createTestSet(input)
    val outputFormat = env.createTestOutputFormat(new Verifier(List(1, 2, 3, 4, 5)))
    dataSet.output(outputFormat)
    env.executeTest()
  }


  it should "handle more than one outputFormat" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
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
    an[TestFailedException] shouldBe thrownBy(env.executeTest())

  }

  it should "handle one failure with multiple outputFormats" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
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
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)

    val evenOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(1, 4, 6, 8)))
    val oddOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(2, 3, 5, 7)))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    //TODO shutdown at failure
    an[TestFailedException] shouldBe thrownBy(env.executeTest())
  }

  it should "not stop if only one trigger fires with multiple outputFormats" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    env.setTimeoutInterval(10000)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)

    val evenOutputFormat = env.createTestOutputFormat(new CountVerifier[Integer](2), new CountTrigger(2))
    val oddOutputFormat = env.createTestOutputFormat(new Verifier[Integer](List(1, 3, 5, 7)))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    env.executeTest()
  }

  it should "stop if all triggers fire" in {
    val env = DataSetTestEnvironment.createTestEnvironment(1)
    val evenlist = List[Integer](2, 4, 6, 8)
    val oddlist = List[Integer](1, 3, 5, 7)
    val evenDataSet = env.fromElements(evenlist: _*)
    val oddDataSet = env.fromElements(oddlist: _*)


    val evenOutputFormat = env.createTestOutputFormat(new CountVerifier[Integer](2), new CountTrigger(2))
    val oddOutputFormat = env.createTestOutputFormat(new CountVerifier[Integer](2), new CountTrigger(2))
    evenDataSet.output(evenOutputFormat)
    oddDataSet.output(oddOutputFormat)
    env.executeTest()
    //check for flag

  }


}
