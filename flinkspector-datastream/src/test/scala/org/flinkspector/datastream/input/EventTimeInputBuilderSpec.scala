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
package org.flinkspector.datastream.input

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.flinkspector.CoreSpec
import org.flinkspector.datastream.input.time.{After, InWindow}

import scala.collection.JavaConversions._

class EventTimeInputBuilderSpec extends CoreSpec {

  trait EventTimeInputBuilderCase {
    val builder = EventTimeInputBuilder.startWith(1)
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

  it should "set flushWindows" in
    new EventTimeInputBuilderCase {

      builder.getFlushWindowsSetting shouldBe false
      builder.flushOpenWindowsOnTermination()
      builder.getFlushWindowsSetting shouldBe true
    }

  it should "repeat a input list two times" in
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

  it should "repeat an element four times" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, After.period(1, TimeUnit.SECONDS), 4)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 1000),
      new StreamRecord[Integer](2, 2000),
      new StreamRecord[Integer](2, 3000),
      new StreamRecord[Integer](2, 4000)
    )
  }

  it should "put elements in windows" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, InWindow.to(1, TimeUnit.SECONDS))
      .emit(3)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 999),
      new StreamRecord[Integer](3, 1000)
    )
  }

  it should "repeat elements in nested windows" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, InWindow.to(1, TimeUnit.SECONDS))
      .emit(3)
      .repeatAll(After.period(0, TimeUnit.DAYS), 2)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 999),
      new StreamRecord[Integer](3, 1000),
      new StreamRecord[Integer](1, 1000),
      new StreamRecord[Integer](2, 1999),
      new StreamRecord[Integer](3, 2000),
      new StreamRecord[Integer](1, 2000),
      new StreamRecord[Integer](2, 2999),
      new StreamRecord[Integer](3, 3000)
    )
  }

  it should "repeat elements with appended window" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, After.period(1, TimeUnit.SECONDS))
      .emit(3, InWindow.to(2, TimeUnit.SECONDS))
      .repeatAll(After.period(0, TimeUnit.DAYS), 1)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 1000),
      new StreamRecord[Integer](3, 1999),
      new StreamRecord[Integer](1, 2000),
      new StreamRecord[Integer](2, 3000),
      new StreamRecord[Integer](3, 3999)
    )
  }

  it should "repeat elements in windows" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, InWindow.to(1, TimeUnit.SECONDS), 3)
      .emit(3)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 999),
      new StreamRecord[Integer](2, 999),
      new StreamRecord[Integer](2, 999),
      new StreamRecord[Integer](3, 1000)
    )
  }

  it should "repeat elements with windows in front" in {
    val builder = EventTimeInputBuilder.startWith(1, InWindow.to(2, TimeUnit.SECONDS))
      .repeatAll(After.period(0, TimeUnit.SECONDS), 2)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 1999),
      new StreamRecord[Integer](1, 2000),
      new StreamRecord[Integer](1, 2000)
    )
  }

  it should "repeat elements with windows in front and back" in {
    val builder = EventTimeInputBuilder.startWith(1, InWindow.to(2, TimeUnit.SECONDS))
      .emit(2, InWindow.to(4, TimeUnit.SECONDS))
      .repeatAll(After.period(0, TimeUnit.SECONDS), 2)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 1999),
      new StreamRecord[Integer](2, 3999),
      new StreamRecord[Integer](1, 4000),
      new StreamRecord[Integer](2, 5999),
      new StreamRecord[Integer](1, 6000),
      new StreamRecord[Integer](2, 7999)
    )
  }

  it should "repeat elements with windows in back" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, InWindow.to(2, TimeUnit.SECONDS))
      .repeatAll(After.period(0, TimeUnit.SECONDS), 2)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 1999),
      new StreamRecord[Integer](1, 2000),
      new StreamRecord[Integer](2, 3999),
      new StreamRecord[Integer](1, 4000),
      new StreamRecord[Integer](2, 5999)
    )
  }

  it should "repeat elements with windows in back using Instant" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, InWindow.to(2, TimeUnit.SECONDS))
      .repeatAll(2)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 1999),
      new StreamRecord[Integer](1, 2000),
      new StreamRecord[Integer](2, 3999),
      new StreamRecord[Integer](1, 4000),
      new StreamRecord[Integer](2, 5999)
    )
  }

  it should "repeat elements using Instant" in {
    val builder = EventTimeInputBuilder.startWith(1, After.period(1, TimeUnit.SECONDS))
      .repeatAll(5)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 1000),
      new StreamRecord[Integer](1, 1000),
      new StreamRecord[Integer](1, 1000),
      new StreamRecord[Integer](1, 1000),
      new StreamRecord[Integer](1, 1000),
      new StreamRecord[Integer](1, 1000)
    )
  }

  it should "repeat elements with windows in back with time in between" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, InWindow.to(2, TimeUnit.SECONDS))
      .repeatAll(After.period(1, TimeUnit.SECONDS), 2)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 1999),
      new StreamRecord[Integer](1, 3000),
      new StreamRecord[Integer](2, 4999),
      new StreamRecord[Integer](1, 6000),
      new StreamRecord[Integer](2, 7999)
    )
  }

  it should "repeat elements with windows in back with double repeat" in {
    val builder = EventTimeInputBuilder.startWith(1)
      .emit(2, InWindow.to(2, TimeUnit.SECONDS))
      .repeatAll(After.period(0, TimeUnit.SECONDS), 2)
      .repeatAll(After.period(0, TimeUnit.SECONDS), 1)


    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 0),
      new StreamRecord[Integer](2, 1999),
      new StreamRecord[Integer](1, 2000),
      new StreamRecord[Integer](2, 3999),
      new StreamRecord[Integer](1, 4000),
      new StreamRecord[Integer](2, 5999),
      new StreamRecord[Integer](1, 6000),
      new StreamRecord[Integer](2, 7999),
      new StreamRecord[Integer](1, 8000),
      new StreamRecord[Integer](2, 9999),
      new StreamRecord[Integer](1, 10000),
      new StreamRecord[Integer](2, 11999)
    )
  }

  it should "repeat elements with windows in front and back double repeat" in {
    val builder = EventTimeInputBuilder.startWith(1, InWindow.to(2, TimeUnit.SECONDS))
      .emit(2, InWindow.to(4, TimeUnit.SECONDS))
      .repeatAll(After.period(0, TimeUnit.SECONDS), 2)
      .repeatAll(After.period(0, TimeUnit.SECONDS), 1)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 1999),
      new StreamRecord[Integer](2, 3999),
      new StreamRecord[Integer](1, 4000),
      new StreamRecord[Integer](2, 5999),
      new StreamRecord[Integer](1, 6000),
      new StreamRecord[Integer](2, 7999),

      new StreamRecord[Integer](1, 8000),
      new StreamRecord[Integer](2, 9999),
      new StreamRecord[Integer](1, 10000),
      new StreamRecord[Integer](2, 11999),
      new StreamRecord[Integer](1, 12000),
      new StreamRecord[Integer](2, 13999)
    )
  }

  it should "repeat elements with windows in front and back tripple repeat" in {
    val builder = EventTimeInputBuilder.startWith(1, InWindow.to(2, TimeUnit.SECONDS))
      .emit(2, InWindow.to(4, TimeUnit.SECONDS))
      .repeatAll(After.period(0, TimeUnit.SECONDS), 2)
      .repeatAll(After.period(0, TimeUnit.SECONDS), 2)

    builder.getInput.toList shouldBe List(
      new StreamRecord[Integer](1, 1999),
      new StreamRecord[Integer](2, 3999),
      new StreamRecord[Integer](1, 4000),
      new StreamRecord[Integer](2, 5999),
      new StreamRecord[Integer](1, 6000),
      new StreamRecord[Integer](2, 7999),

      new StreamRecord[Integer](1, 8000),
      new StreamRecord[Integer](2, 9999),
      new StreamRecord[Integer](1, 10000),
      new StreamRecord[Integer](2, 11999),
      new StreamRecord[Integer](1, 12000),
      new StreamRecord[Integer](2, 13999),

      new StreamRecord[Integer](1, 14000),
      new StreamRecord[Integer](2, 15999),
      new StreamRecord[Integer](1, 16000),
      new StreamRecord[Integer](2, 17999),
      new StreamRecord[Integer](1, 18000),
      new StreamRecord[Integer](2, 19999)
    )
  }


}
