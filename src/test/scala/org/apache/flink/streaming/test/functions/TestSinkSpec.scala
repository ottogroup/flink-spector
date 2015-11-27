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

package org.apache.flink.streaming.test.functions

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.test.CoreSpec
import org.apache.flink.core.runtime.MessageType
import org.apache.flink.core.runtime.MessageType._
import org.apache.flink.core.util.SerializeUtil
import org.mockito.Mockito._
import org.zeromq.ZMQ

import scala.collection.mutable.ArrayBuffer

class TestSinkSpec extends CoreSpec {

  "The sink" should "send output" in new TestSinkCase(1) {

    sink(0).open(config)
    sink(0).invoke("hello")
    sink(0).invoke("world")
    sink(0).close()

    subscriber.recvStr() shouldBe "OPEN 0 1"
    val out = SER.getPayload(subscriber.recv())
    val ser = SerializeUtil.deserialize(out)
    processRec(subscriber.recv(),ser) shouldBe "hello"
    processRec(subscriber.recv(),ser) shouldBe "world"
    subscriber.recvStr() shouldBe "CLOSE 0"

    subscriber.close()
    context.close()
  }

  "The sink" should "send output in parallel" in new TestSinkCase(2) {

    sink(0).open(config)
    sink(1).open(config)

    sink(0).invoke("hello")
    subscriber.recvStr() shouldBe "OPEN 0 2"
    val out = subscriber.recv()
    MessageType.getMessageType(out) shouldBe SER
    val ser = SerializeUtil.deserialize(SER.getPayload(out))
    processRec(subscriber.recv(),ser) shouldBe "hello"

    sink(1).invoke("world")
    subscriber.recvStr() shouldBe "OPEN 1 2"
    getMessageType(subscriber.recv()) shouldBe SER
    processRec(subscriber.recv(),ser) shouldBe "world"

    sink(0).close()
    subscriber.recvStr() shouldBe "CLOSE 0"

    sink(1).close()
    subscriber.recvStr() shouldBe "CLOSE 1"

    subscriber.close()
    context.close()
  }

  class TestSinkCase(parallelism : Int) {

    val contexts : ArrayBuffer[StreamingRuntimeContext] =
      ArrayBuffer.empty[StreamingRuntimeContext]
    for(i <- 0 to parallelism) {
      val runtimeContext = mock[StreamingRuntimeContext]
      when(runtimeContext.getNumberOfParallelSubtasks).thenReturn(parallelism)
      when(runtimeContext.getIndexOfThisSubtask).thenReturn(i)
      contexts += runtimeContext
    }

    val sink = contexts.map{ c =>
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


  def processRec(bytes : Array[Byte],
                 ser: TypeSerializer[Nothing]) : String = {
    SerializeUtil.deserialize(REC.getPayload(bytes),ser)
  }

}
