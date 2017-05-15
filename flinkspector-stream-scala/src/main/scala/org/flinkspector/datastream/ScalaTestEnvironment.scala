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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.test.util.{ForkableFlinkMiniCluster, TestBaseUtils}
import org.flinkspector.core.input.Input
import org.flinkspector.core.runtime.OutputVerifier
import org.flinkspector.core.trigger.VerifyFinishedTrigger
import org.flinkspector.datastream.functions.TestSink
import org.flinkspector.datastream.input.EventTimeInput

import _root_.scala.language.implicitConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class ScalaTestEnvironment(javaEnv: DataStreamTestEnvironment, cluster: ForkableFlinkMiniCluster)
  extends StreamExecutionEnvironment(javaEnv) {
  /**
   * Factory method to create a new instance, providing a
   * new instance of {@link ForkableFlinkMiniCluster}
   *
   * @param parallelism global setting for parallel execution.
   * @return new instance of { @link DataStreamTestEnvironment}
   * @throws Exception
   */
  @throws(classOf[Exception])
  def createTestEnvironment(parallelism: Int): DataStreamTestEnvironment = {
    val cluster: ForkableFlinkMiniCluster = TestBaseUtils.startCluster(1, parallelism, StreamingMode.STREAMING, false, false, true)
    return new DataStreamTestEnvironment(cluster, 1)
  }

  /**
   * Creates a TestSink to verify your the output of your stream.
   * Using a {@link OutputVerifier}
   *
   * @param verifier { @link OutputVerifier} which will be
   *                 used to verify the received records.
   * @return the created sink.
   */
  def createTestSink[IN](verifier: OutputVerifier[IN]): TestSink[IN] = {
    javaEnv.createTestSink(verifier)
  }

  /**
   * Creates a TestSink to verify the output of your stream.
   * The environment will register a port
   *
   * @param verifier which will be used to verify the received records
   * @return the created sink.
   */
  def createTestSink[IN](verifier: OutputVerifier[IN], trigger: VerifyFinishedTrigger[_]): TestSink[IN] = {
    javaEnv.createTestSink(verifier, trigger)
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
   * @param data  The array of elements to create the data stream from.
   * @return The data stream representing the given array of elements
   */
  @SafeVarargs def fromElementsWithTimeStamp[OUT: ClassTag : TypeInformation](data: StreamRecord[OUT]*): DataStreamSource[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    javaEnv.fromCollectionWithTimestamp(data.toList)
  }

  /**
   * Creates a data stream form the given non-empty {@link EventTimeInput} object.
   * The type of the data stream is that of the {@link EventTimeInput}.
   * @param input The { @link EventTimeInput} to create the data stream from.
   * @return The data stream representing the given input.
   */
  def fromInput[OUT: ClassTag : TypeInformation](input: EventTimeInput[OUT]): DataStreamSource[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    javaEnv.fromInput(input,typeInfo)
  }

  /**
   * Creates a data stream form the given non-empty {@link Input} object.
   * The type of the data stream is that of the {@link Input}.
   * @param input The { @link Input} to create the data stream from.
   * @return The data stream representing the given input.
   */
  def fromInput[OUT: ClassTag : TypeInformation](input: Input[OUT]): DataStreamSource[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    javaEnv.fromInput(input,typeInfo)
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
   * @param data  The collection of elements to create the data stream from.
   * @return The data stream representing the given collection
   */
  def fromCollectionWithTimestamp[OUT: ClassTag : TypeInformation](data: List[StreamRecord[OUT]]): DataStreamSource[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    javaEnv.fromCollectionWithTimestamp(data,typeInfo)
  }

  def fromInput[OUT: ClassTag : TypeInformation](input: List[OUT]): DataStreamSource[OUT] = {
    val typeInfo = implicitly[TypeInformation[OUT]]
    javaEnv.fromInput(input,typeInfo)
  }

  /**
   * This method can be used to check if the environment has been
   * stopped prematurely by e.g. a timeout.
   *
   * @return true if has been stopped forcefully.
   */
  def hasBeenStopped: Boolean = {
    javaEnv.hasBeenStopped
  }

  /**
   * Getter for the timeout interval
   * after the test execution gets stopped.
   *
   * @return timeout in milliseconds
   */
  def getTimeoutInterval: Long = {
    javaEnv.getTimeoutInterval
  }

  /**
   * Setter for the timeout interval
   * after the test execution gets stopped.
   *
   * @param interval
   */
  def setTimeoutInterval(interval: Long) {
    javaEnv.setTimeoutInterval(interval)
  }

  @throws(classOf[Throwable])
  def executeTest() {
    javaEnv.executeTest()
  }

}

object ScalaTestEnvironment {

  def create(parallelism: Int): ScalaTestEnvironment = {
    val cluster: ForkableFlinkMiniCluster = TestBaseUtils.startCluster(1, parallelism, StreamingMode.STREAMING, false, false, true)
    new ScalaTestEnvironment(new DataStreamTestEnvironment(cluster, 1), cluster)
  }

}