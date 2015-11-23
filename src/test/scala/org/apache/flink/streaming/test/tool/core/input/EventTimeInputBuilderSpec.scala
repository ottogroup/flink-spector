/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.test.tool.core.input

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.test.tool.CoreSpec

import scala.collection.JavaConversions._

class EventTimeInputBuilderSpec extends CoreSpec {

  trait EventTimeInputBuilderCase {
    val builder = EventTimeInputBuilder.create(1)
      .emit(2, After.period(2, TimeUnit.SECONDS))
      .emit(3, After.period(1, TimeUnit.SECONDS))
      .emit(4, After.period(3, TimeUnit.SECONDS))

  }


  "the builder" should "repeat a input list one times" in
    new EventTimeInputBuilderCase {
      builder.repeatAll(After.period(2, TimeUnit.SECONDS), 1)
      builder.getInput.toList shouldBe List(
        new StreamRecord[Integer](1, 0),
        new StreamRecord[Integer](2, 2000),
        new StreamRecord[Integer](3, 3000),
        new StreamRecord[Integer](4, 6000),
        new StreamRecord[Integer](1, 8000),
        new StreamRecord[Integer](2, 10000),
        new StreamRecord[Integer](3, 11000),
        new StreamRecord[Integer](4, 14000)
      )
    }

  "the builder" should "repeat a input list two times" in
    new EventTimeInputBuilderCase {
      builder.repeatAll(After.period(2, TimeUnit.SECONDS), 2)
      builder.getInput.toList shouldBe List(
        new StreamRecord[Integer](1, 0),
        new StreamRecord[Integer](2, 2000),
        new StreamRecord[Integer](3, 3000),
        new StreamRecord[Integer](4, 6000),
        new StreamRecord[Integer](1, 8000),
        new StreamRecord[Integer](2, 10000),
        new StreamRecord[Integer](3, 11000),
        new StreamRecord[Integer](4, 14000),
        new StreamRecord[Integer](1, 16000),
        new StreamRecord[Integer](2, 18000),
        new StreamRecord[Integer](3, 19000),
        new StreamRecord[Integer](4, 22000)
      )
    }

  "the builder" should "repeat an element two times" in {
    val builder = EventTimeInputBuilder.create(1)
      .emit(2,After.period(1,TimeUnit.SECONDS),4)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 1000),
      new StreamRecord[Integer](2, 2000),
      new StreamRecord[Integer](2, 3000),
      new StreamRecord[Integer](2, 4000)
    )
  }


}
