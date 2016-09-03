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

package org.flinkspector.core.runtime

import com.google.common.primitives.Bytes
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.test.util.ForkableFlinkMiniCluster
import org.flinkspector.core.CoreSpec
import org.flinkspector.core.trigger.VerifyFinishedTrigger
import org.flinkspector.core.util.SerializeUtil
import org.mockito.Mockito._
import org.scalatest.time.SpanSugar._
import org.zeromq.ZMQ

class RunnerSpec extends CoreSpec {

  val config = new ExecutionConfig()
  val typeInfo: TypeInformation[String] = TypeExtractor.getForObject("test")
  val serializer = typeInfo.createSerializer(config)

  "The runner" should "handle output from one sink" in new RunnerCase {
    val runner: Runner = new Runner(cluster) {
      override protected def executeEnvironment(): Unit = {
        //open a socket to push data
        val context = ZMQ.context(1)
        val publisher = context.socket(ZMQ.PUSH)
        publisher.connect("tcp://localhost:" + 5555)

        val msg = Bytes.concat("OPEN 0 1 ;".getBytes, SerializeUtil.serialize(serializer))
        publisher.send(msg, 0)
        sendString(publisher, "1")
        sendString(publisher, "2")
        sendString(publisher, "3")
        publisher.send("CLOSE 0 3", 0)

        publisher.close()
        context.term()
      }
    }

    runner.registerListener(verifier, trigger)
    runner.executeTest()

    verify(verifier).init()
    verify(verifier).receive("1")
    verify(verifier).receive("2")
    verify(verifier).receive("3")
    verify(verifier).finish()

  }

  it should "handle output from parallel sinks" in new RunnerCase {

    val runner: Runner = new Runner(cluster) {
      override protected def executeEnvironment(): Unit = {
        //open a socket to push data
        val context = ZMQ.context(1)
        val publisher = context.socket(ZMQ.PUSH)
        publisher.connect("tcp://localhost:" + 5555)

        val ser = (x: String) =>
          Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
        publisher.send(ser("OPEN 0 3 "), 0)
        publisher.send(ser("OPEN 1 3 "), 0)
        publisher.send(ser("OPEN 2 3 "), 0)
        sendString(publisher, "1")
        publisher.send("CLOSE 0 1", 0)
        sendString(publisher, "2")
        publisher.send("CLOSE 1 1", 0)
        sendString(publisher, "3")
        publisher.send("CLOSE 2 1", 0)

        publisher.close()
        context.term()
      }
    }

    runner.registerListener(verifier, trigger)
    runner.executeTest()

    verify(verifier).init()
    verify(verifier).receive("1")
    verify(verifier).receive("2")
    verify(verifier).receive("3")
    verify(verifier).finish()

  }


  it should "terminate early if finished trigger fired" in new RunnerCase {
    val runner: Runner = new Runner(cluster) {
      override protected def executeEnvironment(): Unit = {
        //open a socket to push data
        val context = ZMQ.context(1)
        val publisher = context.socket(ZMQ.PUSH)
        publisher.connect("tcp://localhost:" + 5555)

        val ser = (x: String) =>
          Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
        publisher.send(ser("OPEN 0 2 "), 0)
        publisher.send(ser("OPEN 1 2 "), 0)
        sendString(publisher, "1")
        publisher.send("CLOSE 0 1", 0)
        sendString(publisher, "2")
        publisher.send("CLOSE 1 1", 0)
        sendString(publisher, "3")
        publisher.send("CLOSE 2 1", 0)

        publisher.close()
        context.term()
      }
    }

    runner.registerListener(verifier, countTrigger)
    runner.executeTest()

    verify(verifier).init()
    verify(verifier).receive("1")
    verify(verifier).receive("2")
    verify(verifier).finish()
  }

  it should "throw a timeout if finished trigger fired" in new RunnerCase {
    val runner: Runner = new Runner(cluster) {
      override protected def executeEnvironment(): Unit = {
        //open a socket to push data
        val context = ZMQ.context(1)
        val publisher = context.socket(ZMQ.PUSH)
        publisher.connect("tcp://localhost:" + 5555)

        val ser = (x: String) =>
          Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
        publisher.send(ser("OPEN 0 2 "), 0)
        publisher.send(ser("OPEN 1 2 "), 0)
        sendString(publisher, "1")
        publisher.send("CLOSE 0 1", 0)
        sendString(publisher, "2")
        sendString(publisher, "3")
        Thread.sleep(1000)
      }
    }
    runner.setTimeoutInterval(500)
    runner.registerListener(verifier, countTrigger)
    runner.executeTest()

    verify(verifier).init()
    verify(verifier).receive("1")
    verify(verifier).receive("2")
    verify(verifier).finish()
  }


  ignore should "stop with a timeout and sleep" in new RunnerCase {
    val runner: Runner = new Runner(cluster) {
      override protected def executeEnvironment(): Unit = {
        //open a socket to push data
        val context = ZMQ.context(1)
        val publisher = context.socket(ZMQ.PUSH)
        publisher.connect("tcp://localhost:" + 5555)

        val ser = (x: String) =>
          Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
        publisher.send(ser("OPEN 0 2 "), 0)
        publisher.send(ser("OPEN 1 2 "), 0)
        sendString(publisher, "1")
        publisher.send("CLOSE 0 1", 0)
        sendString(publisher, "2")
        sendString(publisher, "3")
        Thread.sleep(2000)
        sendString(publisher, "4")
      }
    }
    runner.setTimeoutInterval(500)
    runner.registerListener(verifier, trigger)
    runner.executeTest()

    verify(verifier).init()
    verify(verifier).receive("1")
    verify(verifier).receive("2")
    verify(verifier).receive("3")
    verify(verifier).finish()
    verifyNoMoreInteractions(verifier)
  }

  it should "stop with a timeout" in new RunnerCase {
    val runner: Runner = new Runner(cluster) {
      override protected def executeEnvironment(): Unit = {
        //open a socket to push data
        val context = ZMQ.context(1)
        val publisher = context.socket(ZMQ.PUSH)
        publisher.connect("tcp://localhost:" + 5555)

        val ser = (x: String) =>
          Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
        publisher.send(ser("OPEN 0 2 "), 0)
        publisher.send(ser("OPEN 1 2 "), 0)
        sendString(publisher, "1")
        publisher.send("CLOSE 0 1", 0)
        sendString(publisher, "2")
        sendString(publisher, "3")
      }
    }
    runner.setTimeoutInterval(500)
    runner.registerListener(verifier, trigger)
    failAfter(2000 millis) {
      runner.executeTest()
    }

    runner.hasBeenStopped shouldBe true

    verify(verifier).init()
    verify(verifier).receive("1")
    verify(verifier).receive("2")
    verify(verifier).receive("3")
    verify(verifier).finish()
  }

  def sendString(socket: ZMQ.Socket, msg: String): Unit = {
    val bytes = SerializeUtil.serialize(msg, serializer)
    val packet = Bytes.concat("REC".getBytes, bytes)
    socket.send(packet, 0)
  }


  trait RunnerCase {

    val verifier = mock[OutputVerifier[String]]
    val cluster = mock[ForkableFlinkMiniCluster]

    val trigger = new VerifyFinishedTrigger[String] {
      override def onRecord(record: String): Boolean = false

      override def onRecordCount(count: Long): Boolean = false
    }

    val countTrigger = new VerifyFinishedTrigger[String] {
      override def onRecord(record: String): Boolean = false

      override def onRecordCount(count: Long): Boolean = count >= 2
    }

  }


}
