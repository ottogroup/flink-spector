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
package org.flinkspector.core.util

import org.flinkspector.core.CoreSpec
import org.flinkspector.util.Util

class UtilSpec extends CoreSpec {

  "prepend" should "prepend an element to an array" in {
    val array = Array("2", "3", "4", "5")
    Util.prepend("1", array) shouldBe Array("1", "2", "3", "4", "5")
  }

}
