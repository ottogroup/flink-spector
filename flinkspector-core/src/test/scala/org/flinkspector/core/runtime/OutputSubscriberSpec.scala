///*
// * Copyright 2015 Otto (GmbH & Co KG)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.flinkspector.core.runtime
//
//import com.google.common.primitives.Bytes
//import org.apache.flink.api.common.ExecutionConfig
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.typeutils.TypeExtractor
//import org.flinkspector.core.CoreSpec
//import org.flinkspector.core.runtime.OutputHandler.ResultState
//import org.flinkspector.core.trigger.VerifyFinishedTrigger
//import org.flinkspector.core.util.SerializeUtil
//import org.mockito.Mockito._
//import org.zeromq.{ZContext, ZMQ}
//
//class OutputSubscriberSpec extends CoreSpec {
//
//  val config = new ExecutionConfig()
//  val typeInfo: TypeInformation[String] = TypeExtractor.getForObject("test")
//  val serializer = typeInfo.createSerializer(config)
//
//  "The listener" should "handle output from one sink" in new OutputListenerCase {
//    val listener = new OutputHandler[String](subscriber, verifier, trigger)
//
//    val msg = Bytes.concat("OPEN 0 1 ;".getBytes, SerializeUtil.serialize(serializer))
//    publisher.send(msg, 0)
//    sendString(publisher, "1")
//    sendString(publisher, "2")
//    sendString(publisher, "3")
//    publisher.send("CLOSE 0 3", 0)
//
//    listener.call() shouldBe ResultState.SUCCESS
//
//    verify(verifier).init()
//    verify(verifier).receive("1")
//    verify(verifier).receive("2")
//    verify(verifier).receive("3")
//    verify(verifier).finish()
//    close()
//  }
//
//  it should "handle output from multiple sinks" in new OutputListenerCase {
//    val listener = new OutputHandler[String](subscriber, verifier, trigger)
//
//    val ser = (x: String) =>
//      Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
//    publisher.send(ser("OPEN 0 3 "), 0)
//    publisher.send(ser("OPEN 1 3 "), 0)
//    publisher.send(ser("OPEN 2 3 "), 0)
//    sendString(publisher, "1")
//    publisher.send("CLOSE 0 1", 0)
//    sendString(publisher, "2")
//    publisher.send("CLOSE 1 1", 0)
//    sendString(publisher, "3")
//    publisher.send("CLOSE 2 1", 0)
//
//    listener.call() shouldBe ResultState.SUCCESS
//
//    verify(verifier).init()
//    verify(verifier).receive("1")
//    verify(verifier).receive("2")
//    verify(verifier).receive("3")
//    verify(verifier).finish()
//    close()
//  }
//
//  it should "throw an Exception if not all sinks were opened" in new OutputListenerCase {
//    val listener = new OutputHandler[String](subscriber, verifier, trigger)
//
//    val ser = (x: String) =>
//      Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
//    publisher.send(ser("OPEN 0 3 "), 0)
//    publisher.send(ser("OPEN 2 3 "), 0)
//    sendString(publisher, "1")
//    publisher.send("CLOSE 0 1", 0)
//    sendString(publisher, "2")
//    publisher.send("CLOSE 1 1", 0)
//    sendString(publisher, "3")
//    publisher.send("CLOSE 2 1", 0)
//
//    an[FlinkTestFailedException] shouldBe thrownBy(listener.call())
//
//    verify(verifier).init()
//    verify(verifier).receive("1")
//    verify(verifier).receive("2")
//    verify(verifier).receive("3")
//    close()
//  }
//
//  it should "terminate early if finished trigger fired" in new OutputListenerCase {
//    val listener = new OutputHandler[String](subscriber, verifier, countTrigger)
//
//    val ser = (x: String) =>
//      Bytes.concat((x + ";").getBytes, SerializeUtil.serialize(serializer))
//    publisher.send(ser("OPEN 0 3 "), 0)
//    publisher.send(ser("OPEN 2 3 "), 0)
//    sendString(publisher, "1")
//    publisher.send("CLOSE 0 1", 0)
//    sendString(publisher, "2")
//    publisher.send("CLOSE 1 1", 0)
//    sendString(publisher, "3")
//    publisher.send("CLOSE 2 1", 0)
//
//    listener.call() shouldBe ResultState.TRIGGERED
//
//    verify(verifier).init()
//    verify(verifier).receive("1")
//    verify(verifier).receive("2")
//    verify(verifier).finish()
//    close()
//  }
//
//  def sendString(socket: ZMQ.Socket, msg: String): Unit = {
//    val bytes = SerializeUtil.serialize(msg, serializer)
//    val packet = Bytes.concat("REC".getBytes, bytes)
//    socket.send(packet, 0)
//  }
//
//  trait OutputListenerCase {
//    val verifier = mock[OutputVerifier[String]]
//    val trigger = new VerifyFinishedTrigger[String] {
//      override def onRecord(record: String): Boolean = false
//
//      override def onRecordCount(count: Long): Boolean = false
//    }
//
//    val countTrigger = new VerifyFinishedTrigger[String] {
//      override def onRecord(record: String): Boolean = false
//
//      override def onRecordCount(count: Long): Boolean = count >= 2
//    }
//
//    //open a socket to push data
//    val context = new ZContext()
//    val publisher = context.createSocket(ZMQ.PUSH)
//    publisher.connect("tcp://localhost:" + 5557)
//    val context2 = new ZContext()
//    val subscriber: Int = 5557
////    subscriber.bind("tcp://*:" + 5557)
//
//    def close(): Unit = {
////      context.destroySocket(subscriber)
//      context.destroySocket(publisher)
//      context2.close()
//      context.close()
//    }
//  }
//
//
//}
