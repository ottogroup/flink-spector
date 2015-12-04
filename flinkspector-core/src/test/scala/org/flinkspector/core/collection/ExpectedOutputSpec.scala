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

package org.flinkspector.core.collection

import org.flinkspector.core.CoreSpec

import scala.collection.JavaConversions._

class ExpectedOutputSpec extends CoreSpec {

  "expected output" should "check for all per default" in {
    val expectOutput = new ExpectedOutput[Int]

    expectOutput.expectAll(List(1,2,3,4))

    expectOutput.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    expectOutput.matchesSafely(List(1, 2, 3)) shouldBe false
    expectOutput.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe true
  }

  it should "take refinements" in {
    val expectOutput = new ExpectedOutput[Int]

    expectOutput.expectAll(List(1,2,3,4))
    expectOutput.refine().only()

    //specify

    expectOutput.matchesSafely(List(1, 2, 3, 4)) shouldBe true
    expectOutput.matchesSafely(List(1, 2, 3)) shouldBe false
    expectOutput.matchesSafely(List(1, 2, 3, 4, 5)) shouldBe false
  }


}
