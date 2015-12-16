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

package org.flinkspector.core.quantify

import org.flinkspector.core.CoreSpec
import org.hamcrest.StringDescription
import org.hamcrest.core.IsCollectionContaining

import scala.collection.JavaConversions._

class OutputMatcherFactoryCase extends CoreSpec {

  "The factory" should "wrap an existing matcher" in {
    val matcher = IsCollectionContaining.hasItem("test")
    val outputMatcher = OutputMatcherFactory.create[String](matcher)

    outputMatcher.matchesSafely(List("test", "hans")) shouldBe true
    val desc = new StringDescription
    outputMatcher.describeTo(desc)
    desc.toString shouldBe "a collection containing \"test\""
    val mismatch = new StringDescription
    outputMatcher.describeMismatch("hans", mismatch)
    mismatch.toString shouldBe "was a java.lang.String (\"hans\")"

  }

}
