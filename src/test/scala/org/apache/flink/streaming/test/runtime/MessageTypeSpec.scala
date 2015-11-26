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
package org.apache.flink.streaming.test.runtime

import org.apache.flink.streaming.test.CoreSpec
import org.apache.flink.streaming.test.runtime.MessageType._

class MessageTypeSpec extends CoreSpec {

  "getMessageType" should "classify messages" in {
    MessageType
      .getMessageType("OPEN 1 2".getBytes) shouldBe OPEN

    MessageType
      .getMessageType("SERfoobar".getBytes) shouldBe SER

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

  it should "return the payload of a SER message" in {
    val payload = SER.getPayload("SERblabla".getBytes)
    new String(payload) shouldBe "blabla"
  }

  "isType" should "should match the type of a message" in {
    MessageType.isType("SERblabla".getBytes, SER) shouldBe true
    MessageType.isType("SERblabla".getBytes, OPEN) shouldBe false
  }


}
