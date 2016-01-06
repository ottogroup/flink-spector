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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.test.util.{ForkableFlinkMiniCluster, TestBaseUtils}
import org.flinkspector.core.input.Input
import org.flinkspector.core.runtime.OutputVerifier
import org.flinkspector.core.trigger.VerifyFinishedTrigger
import org.flinkspector.datastream.functions.TestSink
import org.flinkspector.datastream.input.EventTimeInput

import _root_.scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class DataStreamTestEnvironment(testEnv: org.flinkspector.datastream.DataStreamTestEnvironment) extends StreamExecutionEnvironment(testEnv) {


  @throws(classOf[Throwable])
  def executeTest() {
    testEnv.executeTest()
  }

  /**
   * Creates a TestSink to verify your the output of your stream.
   * Using a {@link OutputVerifier}
   *
   * @param verifier { @link OutputVerifier} which will be
   *                 used to verify the received records.
   */
  def createTestSink[IN](verifier: OutputVerifier[IN]): TestSink[IN] = {
    testEnv.createTestSink(verifier)
  }

  /**
   * Creates a TestSink to verify the output of your stream.
   * The environment will register a port
   *
   * @param verifier which will be used to verify the received records
   * @return the created sink.
   */
  def createTestSink[IN](verifier: OutputVerifier[IN], trigger: VerifyFinishedTrigger[_]): TestSink[IN] = {
    testEnv.createTestSink(verifier, trigger)
  }


  /**
   * Creates a new data stream that contains the given elements. The elements must all be of the same type, for
   * example, all of the {@link String} or {@link Integer}.
   * <p>
   * The framework will try and determine the exact type from the elements. In case of generic elements, it may be
   * necessary to manually supply the type information via {@link #fromCollection(java.util.Collection,
	 * org.apache.flink.api.common.typeinfo.TypeInformation)}.
   * <p>
   * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
   * degree of parallelism one.
   *
   * @param data  The array of elements to startWith the data stream from.
   * @return The data stream representing the given array of elements
   */
  @SafeVarargs def fromElementsWithTimeStamp[OUT: ClassTag: TypeInformation](data: StreamRecord[OUT]*): DataStream[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    testEnv.fromCollectionWithTimestamp(data.asJava,typeInfo)
  }

  /**
   * Creates a data stream form the given non-empty {@link EventTimeInput} object.
   * The type of the data stream is that of the {@link EventTimeInput}.
   * @param input The { @link EventTimeInput} to startWith the data stream from.
   * @return The data stream representing the given input.
   */
  def fromInput[OUT: ClassTag: TypeInformation](input: EventTimeInput[OUT]): DataStream[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    testEnv.fromInput(input,typeInfo)
  }

  /**
   * Creates a data stream form the given non-empty {@link Input} object.
   * The type of the data stream is that of the {@link Input}.
   * @param input The { @link Input} to startWith the data stream from.
   * @return The data stream representing the given input.
   */
  def fromInput[OUT: ClassTag: TypeInformation](input: Input[OUT]): DataStream[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    testEnv.fromInput(input,typeInfo)
  }

  /**
   * Creates a data stream from the given non-empty collection. The type of the data stream is that of the
   * elements in the collection.
   * <p>
   * <p>The framework will try and determine the exact type from the collection elements. In case of generic
   * elements, it may be necessary to manually supply the type information via
   * {@link #fromCollection(java.util.Collection, org.apache.flink.api.common.typeinfo.TypeInformation)}.</p>
   * <p>
   * <p>Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
   * parallelism one.</p>
   *
   * @param data  The collection of elements to startWith the data stream from.
   * @return The data stream representing the given collection
   */
  def fromCollectionWithTimestamp[OUT: ClassTag: TypeInformation](data: Seq[StreamRecord[OUT]]): DataStream[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    testEnv.fromCollectionWithTimestamp(data.asJava,typeInfo)
  }

  /**
   * This method can be used to check if the environment has been
   * stopped prematurely by e.g. a timeout.
   *
   * @return true if has been stopped forcefully.
   */
  def hasBeenStopped: Boolean = {
    testEnv.hasBeenStopped
  }

  /**
   * Getter for the timeout interval
   * after the test execution gets stopped.
   *
   * @return timeout in milliseconds
   */
  def getTimeoutInterval: Long = {
    testEnv.getTimeoutInterval
  }

  /**
   * Setter for the timeout interval
   * after the test execution gets stopped.
   *
   * @param interval in milliseconds.
   */
  def setTimeoutInterval(interval: Long) {
    testEnv.setTimeoutInterval(interval)
  }

  def close(): Unit = {
    testEnv.terminate()
  }
}

object DataStreamTestEnvironment {
  /**
   * Factory method to startWith a new instance, providing a
   * new instance of {@link ForkableFlinkMiniCluster}
   *
   * @param parallelism global setting for parallel execution.
   * @return new instance of { @link DataStreamTestEnvironment}
   * @throws Exception
   */
  @throws(classOf[Exception])
  def createTestEnvironment(parallelism: Int): DataStreamTestEnvironment = {
    val tasksSlots: Int = Runtime.getRuntime.availableProcessors
    val cluster: ForkableFlinkMiniCluster = TestBaseUtils.startCluster(1, tasksSlots, StreamingMode.STREAMING, false, false, true)
    val env = new org.flinkspector.datastream.DataStreamTestEnvironment(cluster, parallelism)
    new DataStreamTestEnvironment(env)
  }
}
