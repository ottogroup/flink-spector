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

package org.flinkspector.core.trigger

import org.flinkspector.core.CoreSpec
import org.hamcrest.core.Is

class FinishAtMatchSpec extends CoreSpec {

  "The finish at match trigger" should "trigger at a match :)" in {
    val trigger = FinishAtMatch.of(Is.is(1))

    trigger.onRecordCount(999) shouldBe false

    trigger.onRecord(0) shouldBe false
    trigger.onRecord(1) shouldBe true
    trigger.onRecord(2) shouldBe false
  }

}
