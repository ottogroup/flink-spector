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

package org.flinkspector.datastream.functions

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.flinkspector.CoreSpec
import org.flinkspector.core.runtime.MessageType
import org.flinkspector.core.util.SerializeUtil
import org.mockito.Mockito._
import org.zeromq.ZMQ

import scala.collection.mutable.ArrayBuffer

class TestSinkSpec extends CoreSpec {

  "The sink" should "send output" in new TestSinkCase(1) {

    sink(0).open(config)
    sink(0).invoke("hello")
    sink(0).invoke("world")
    sink(0).close()

    val open = processOpen(subscriber.recv())
    val ser = open._2
    open._1 shouldBe "OPEN 0 1"
    processRec(subscriber.recv(), ser) shouldBe "hello"
    processRec(subscriber.recv(), ser) shouldBe "world"
    subscriber.recvStr() shouldBe "CLOSE 0 2"

    subscriber.close()
    context.close()
  }

  it should "send output in parallel" in new TestSinkCase(2) {

    sink(0).open(config)
    sink(1).open(config)

    sink(0).invoke("hello")
    val open1 = processOpen(subscriber.recv())
    open1._1 shouldBe "OPEN 0 2"
    processRec(subscriber.recv(), open1._2) shouldBe "hello"

    sink(1).invoke("world")
    val open2 = processOpen(subscriber.recv())
    open2._1 shouldBe "OPEN 1 2"
    processRec(subscriber.recv(), open2._2) shouldBe "world"

    sink(0).close()
    subscriber.recvStr() shouldBe "CLOSE 0 1"

    sink(1).close()
    subscriber.recvStr() shouldBe "CLOSE 1 1"

    subscriber.close()
    context.close()
  }

  class TestSinkCase(parallelism: Int) {

    val contexts: ArrayBuffer[StreamingRuntimeContext] =
      ArrayBuffer.empty[StreamingRuntimeContext]
    for (i <- 0 to parallelism) {
      val runtimeContext = mock[StreamingRuntimeContext]
      when(runtimeContext.getNumberOfParallelSubtasks).thenReturn(parallelism)
      when(runtimeContext.getIndexOfThisSubtask).thenReturn(i)
      contexts += runtimeContext
    }

    val sink = contexts.map { c =>
      val s = new TestSink[String](5555)
      s.setRuntimeContext(c)
      s
    }

    val config = new Configuration()

    val context: ZMQ.Context = ZMQ.context(1)
    // socket to receive from sink
    val subscriber: ZMQ.Socket = context.socket(ZMQ.PULL)
    subscriber.bind("tcp://*:" + 5555)
  }


  def processRec(bytes: Array[Byte],
                 ser: TypeSerializer[Nothing]): String = {
    SerializeUtil.deserialize(MessageType.REC.getPayload(bytes), ser)
  }

  def processOpen(bytes: Array[Byte]): (String, TypeSerializer[Nothing]) = {
    val msg = new String(bytes, "UTF-8")
    val values: String = msg.split(" ;").head
    val out = MessageType.OPEN.getPayload(bytes)
    val typeSerializer = SerializeUtil.deserialize(out)
    (values, typeSerializer)
  }

}
