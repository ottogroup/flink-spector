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
package org.apache.flink.core.input

import org.apache.flink.streaming.CoreSpec

class InputTranslatorSpec extends CoreSpec{

  class StringToInt(input: Input[String])
    extends InputTranslator[String, Integer](input) {
    override def translate(elem: String): Integer = Integer.parseInt(elem)
  }

  "The translator" should "transform string input into integer input" in {
    val input = new TestInput(List("1","2","3","4","5"))
    new StringToInt(input).getInput should contain only(1,2,3,4,5)
  }
}
