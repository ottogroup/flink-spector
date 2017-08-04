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
package io.flinkspector.dataset

import java.util.{List => JList}

import io.flinkspector.core.input.Input
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


  class TestInput[T](input: List[T]) extends Input[T] {
    override def getInput: JList[T] = input
  }

}
