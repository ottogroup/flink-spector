package org.apache.flink.streaming.test.tool.runtime.output

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.streaming.test.tool.CoreSpec
import org.apache.flink.streaming.test.tool.runtime.messaging.MessageType
import org.apache.flink.streaming.test.tool.util.SerializeUtil
import org.zeromq.ZMQ
import org.mockito.Mockito._
import MessageType._

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
  }

  "The sink" should "send output in parallel" in new TestSinkCase(2) {

    sink(0).open(config)
    sink(1).open(config)
    sink(0).invoke("hello")
    Thread.sleep(500)
    sink(1).invoke("world")
    sink(0).close()
    sink(1).close()

    subscriber.recvStr() shouldBe "OPEN 0 2"
    subscriber.recvStr() shouldBe "OPEN 1 2"
    //first serializer
    val out = SER.getPayload(subscriber.recv())
    val ser = SerializeUtil.deserialize(out)
    getMessageType(subscriber.recv()) shouldBe SER
    processRec(subscriber.recv(),ser) shouldBe "hello"
    processRec(subscriber.recv(),ser) shouldBe "world"
    subscriber.recvStr() shouldBe "CLOSE 0"
    subscriber.recvStr() shouldBe "CLOSE 1"
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
