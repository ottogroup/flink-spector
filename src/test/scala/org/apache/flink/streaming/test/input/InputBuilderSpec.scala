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

package org.apache.flink.streaming.test.input

import org.apache.flink.streaming.test.CoreSpec

class InputBuilderSpec extends CoreSpec {

  "The builder" should "produce a list of input" in {
    val input = new InputBuilder[Int]
    input.emit(1)
    input.emit(2)
    input.emit(3).emit(4)
    input.getInput should contain only(1, 2, 3, 4)
  }

  it should "repeat a single record" in {
    val input = new InputBuilder[Int]
    input.emit(1)
    input.emit(2)
    input.emit(3, 3)

    input.getInput should contain theSameElementsAs List(1, 2, 3, 3, 3)
  }

  it should "repeat a the whole list" in {
    val input = new InputBuilder[Int]
    input.emit(1)
    input.emit(2)
    input.repeatAll(2)
    input.emit(3)

    input.getInput should contain theSameElementsAs List(1, 2, 1, 2, 1, 2, 3)
  }

}
