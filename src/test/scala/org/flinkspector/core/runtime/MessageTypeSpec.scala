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
import org.flinkspector.CoreSpec
import org.flinkspector.core.runtime.MessageType._
import org.flinkspector.core.util.SerializeUtil

class MessageTypeSpec extends CoreSpec {

  "getMessageType" should "classify messages" in {
    MessageType
      .getMessageType("OPEN 1 2".getBytes) shouldBe OPEN

    MessageType
      .getMessageType("RECMarostn".getBytes) shouldBe REC

    MessageType
      .getMessageType("CLOSE 1".getBytes) shouldBe CLOSE

    an[UnsupportedOperationException] shouldBe thrownBy {
      MessageType
        .getMessageType("EDN 1".getBytes)
    }
  }

  "getPayload" should "return the payload of a REC message" in {
    val payload = REC.getPayload("RECblabla".getBytes)
    new String(payload) shouldBe "blabla"
  }

  it should "return the payload of a OPEN message" in {
    val payload = OPEN.getPayload("OPEN 2 12 ;blabla".getBytes)
    new String(payload) shouldBe "blabla"

    val config = new ExecutionConfig()
    val typeInfo: TypeInformation[String] = TypeExtractor.getForObject("test")
    val serializer = typeInfo.createSerializer(config)

    val ser = OPEN.getPayload(Bytes.concat("OPEN 2 12 ;".getBytes,SerializeUtil.serialize(serializer)))

    SerializeUtil.deserialize(ser) shouldBe serializer
  }

  "isType" should "should match the type of a message" in {
    MessageType.isType("CLOSEblabla".getBytes, CLOSE) shouldBe true
    MessageType.isType("CLOSEblabla".getBytes, OPEN) shouldBe false
  }


}
