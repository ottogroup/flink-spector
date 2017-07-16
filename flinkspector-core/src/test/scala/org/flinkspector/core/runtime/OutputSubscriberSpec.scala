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

import java.util.concurrent.Executors

import com.lmax.disruptor.dsl.Disruptor
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.flinkspector.core.CoreSpec
import org.mortbay.util.IO.bufferSize

class OutputSubscriberSpec extends CoreSpec {

  val config = new ExecutionConfig()
  val typeInfo: TypeInformation[String] = TypeExtractor.getForObject("test")
  val serializer = typeInfo.createSerializer(config)

  "The subscriber" should "receive a message" in new OutputListenerCase {

    publisher.send("hello")
    listener.recvStr() shouldBe("hello")

    listener.close()
    close()
  }

  "The subscriber" should "receive two messages" in new OutputListenerCase {

    publisher.send("hello")
    listener.recvStr() shouldBe("hello")
    publisher.send("world")
    listener.recvStr() shouldBe("world")

    listener.close()
    close()
  }

  "The subscriber" should "receive 10 messages" in new OutputListenerCase {

    for (i <- 1 to 10) {
      publisher.send(s"hello$i")
    }

    var out = List.empty[String]
    for(i <- 1 to 10) {
      println(i)
      out = listener.recvStr() :: out
    }
    out should have length(10)

    listener.close()
    close()
  }

  trait OutputListenerCase {

    val executor = Executors.newCachedThreadPool

    val factory = new ByteEventFactory

    val disruptor = new Disruptor[ByteEvent](factory, bufferSize, executor)

    //open a socket to push data
    val publisher = new OutputPublisher(1, disruptor.getRingBuffer)

    val listener = new OutputSubscriber(1, disruptor)

    disruptor.start()

    def close(): Unit = {
      //      subscriber.close()
      publisher.close()
    }
  }


}
