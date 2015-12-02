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
package org.flinkspector.datastream

import java.util.{List => JList}

import org.apache.flink.streaming.runtime.streamrecord
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.flinkspector.core.input.Input
import org.flinkspector.datastream.input.EventTimeInput
import org.scalatest.concurrent.Timeouts
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.collection.JavaConversions._

abstract class CoreSpec
  extends FlatSpec
  with Matchers
  with OptionValues
  with MockitoSugar
  with Timeouts {

  class TestEventTimeInput[T](input: List[T]) extends EventTimeInput[T] {
    override def getInput: JList[StreamRecord[T]] =
      input.map(new streamrecord.StreamRecord[T](_, 0))
  }

  class TestInput[T](input: List[T]) extends Input[T] {
    override def getInput: JList[T] = input
  }

  def recordsToValues[T](lst: Iterable[StreamRecord[T]]): List[T] =
    lst.map(_.getValue).toList


  def valuesToRecords[T](lst: List[T]): Iterable[StreamRecord[T]] =
    lst.map(new StreamRecord[T](_, 0))

}
