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

package org.flinkspector.scala.datastream

import java.lang.{Iterable => JIterable}
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.flinkspector.core.collection.{MatcherBuilder, ExpectedRecords}
import org.flinkspector.core.input.{InputBuilder, Input}
import org.flinkspector.core.quantify.HamcrestVerifier
import org.flinkspector.core.runtime.OutputVerifier
import org.flinkspector.core.trigger.VerifyFinishedTrigger
import org.flinkspector.datastream.functions.TestSink
import org.flinkspector.datastream.input.{EventTimeInputBuilder, EventTimeInput}
import org.flinkspector.datastream.input.time.{Before, After}
import org.hamcrest.Matcher
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.reflect.ClassTag

trait FlinkDataStream extends BeforeAndAfterEach { this: Suite =>


  /**
   * Test Environment
   */
  private var testEnv: DataStreamTestEnvironment = _

  private var executed = false

  override def beforeEach() {
    testEnv = DataStreamTestEnvironment.createTestEnvironment(1)
    testEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach() {
    try super.afterEach() // To be stackable, must call super.afterEach
    finally {
      testEnv.close()
      executed = false
    }
  }


  def executeTest(): Unit = {
    if(!executed) {
      executed = true
      testEnv.executeTest()
    }
  }

  /**
   * Creates a DataStreamSource from an EventTimeInput object.
   * The DataStreamSource will emit the records with the specified EventTime.
   *
   * @param input to emit.
   * @return a DataStreamSource generating the input.
   */
  def createTestStream[OUT: ClassTag: TypeInformation](input: EventTimeInput[OUT]): DataStream[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    testEnv.fromInput(input)
  }

  /**
   * Creates a DataStreamSource from an EventTimeInput object.
   * The DataStreamSource will emit the records with the specified EventTime.
   *
   * @param input to emit.
   * @return a DataStreamSource generating the input.
   */
  def createTestStream[OUT: ClassTag: TypeInformation](input: Seq[OUT]): DataStream[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    testEnv.fromElements(input: _*)
  }

  /**
   * Creates a DataStreamSource from an Input object.
   *
   * @param input to emit.
   * @return a DataStream generating the input.
   */
  def createTestStream[OUT: ClassTag: TypeInformation](input: Input[OUT]): DataStream[OUT] = {
    testEnv.fromInput(input)
  }

  /**
   * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
   *
   * @param matcher of type JIterable<IN>
   * @return the created sink.
   */
  def createTestSink[IN](matcher: Matcher[JIterable[IN]]): TestSink[IN] = {
    val verifier: OutputVerifier[IN] = new HamcrestVerifier[IN](matcher)
    createTestSink(verifier)
  }

  /**
   * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
   *
   * @param verifier of generic type IN
   * @return the created sink.
   */
  def createTestSink[IN](verifier: OutputVerifier[IN], trigger: VerifyFinishedTrigger[_]): TestSink[IN] = {
    testEnv.createTestSink(verifier, trigger)
  }

  /**
   * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
   *
   * @param verifier of generic type IN
   * @return the created sink.
   */
  def createTestSink[IN](verifier: OutputVerifier[IN]): TestSink[IN] = {
    testEnv.createTestSink(verifier)
  }

  /**
   * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
   *
   * @param matcher of type JIterable<IN>
   * @return the created sink.
   */
  def createTestSink[IN](matcher: Matcher[JIterable[IN]], trigger: VerifyFinishedTrigger[_]): TestSink[IN] = {
    val verifier: OutputVerifier[IN] = new HamcrestVerifier[IN](matcher)
    createTestSink(verifier, trigger)
  }

  /**
   * Sets the parallelism for operations executed through this environment.
   * Setting a parallelism of x here will cause all operators (such as map,
   * batchReduce) to run with x parallel instances. This method overrides the
   * default parallelism for this environment. The
   * {@link LocalStreamEnvironment} uses by default a value equal to the
   * number of hardware contexts (CPU cores / threads). When executing the
   * program via the command line client from a JAR file, the default degree
   * of parallelism is the one configured for that setup.
   *
   * @param parallelism The parallelism
   */
  def setParallelism(parallelism: Int) {
    testEnv.setParallelism(parallelism)
  }

  def fulfill[T](matcher: Matcher[JIterable[T]]) : FulfillWord[T] = {
    new FulfillWord[T](matcher)
  }

  class StreamShouldWrapper[T](val stream: DataStream[T]){
      def should(fulFillWord: FulfillWord[T]) = {
        val matcher = fulFillWord.matcher
        fulFillWord.trigger match {
          case Some(trigger) =>
            stream.addSink(createTestSink(matcher,trigger))
          case None =>
            stream.addSink(createTestSink(matcher))
        }
        executeTest()
      }
  }

  //================================================================================
  // Syntactic sugar stuff
  //================================================================================
  /**
   * Creates an {@link After} object.
   *
   * @param span length of span.
   * @param unit of time.
   * @return { @link After}
   */
  def after(span: Long, unit: TimeUnit): After = {
    return After.period(span, unit)
  }

  /**
   * Creates an {@link Before} object.
   *
   * @param span length of span.
   * @param unit of time.
   * @return { @link Before}
   */
  def before(span: Long, unit: TimeUnit): Before = {
    return Before.period(span, unit)
  }

  def startWith[T](record: T): EventTimeInputBuilder[T] = {
    return EventTimeInputBuilder.create(record)
  }

  def emit[T](elem: T): InputBuilder[T] = {
    return InputBuilder.startWith(elem)
  }

  def expectOutput[T](record: T): ExpectedRecords[T] = {
    return ExpectedRecords.create(record)
  }

  def times(n: Int): Int = {
    return n
  }

  val strict: MatcherBuilder.Order = MatcherBuilder.Order.STRICT
  val notStrict: MatcherBuilder.Order = MatcherBuilder.Order.NONSTRICT
  val seconds: TimeUnit = TimeUnit.SECONDS
  val minutes: TimeUnit = TimeUnit.MINUTES
  val hours: TimeUnit = TimeUnit.HOURS
  val ignore: String = null

  implicit def convertToAnyShouldWrapper[T](o: DataStream[T]): StreamShouldWrapper[T] = new StreamShouldWrapper[T](o)

}

final class FulfillWord[T](val matcher: Matcher[JIterable[T]]) {

  var trigger: Option[VerifyFinishedTrigger[_]] = None

  def withTrigger(trigger: VerifyFinishedTrigger[_]): Unit = {
    this.trigger = Some(trigger)
  }
}
