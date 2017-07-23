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

package org.flinkspector.scala.datastream.input

import org.flinkspector.core.input.{InputBuilder => JInput}
import scala.collection.JavaConversions._

class InputBuilder[T] extends JInput[T] {

  /**
    * Adds a collection of elements to the input
    *
    * @param records times to repeat
    */
  def emitAll(records: Seq[T]): InputBuilder[T] = {
    super.emitAll(records)
    this
  }


}
