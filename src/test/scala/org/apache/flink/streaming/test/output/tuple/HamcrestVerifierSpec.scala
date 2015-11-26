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

package org.apache.flink.streaming.test.output.tuple

import org.apache.flink.streaming.test.CoreSpec
import org.apache.flink.streaming.test.output.HamcrestVerifier
import org.apache.flink.streaming.test.runtime.StreamTestFailedException
import org.hamcrest.core.IsCollectionContaining

import scala.collection.JavaConversions._

class HamcrestVerifierSpec extends CoreSpec {

  "The verifier" should "wrap a hamcrest matcher" in {
    val matcher = IsCollectionContaining.hasItem("test")
    val verifier = new HamcrestVerifier(matcher)

    verifier.verify(List("test","hans"))

    val thrown = the [StreamTestFailedException] thrownBy {
      verifier.verify(List("susi", "hans"))
    }
    thrown.getMessage should include ("test")
    thrown.getCause shouldBe an [AssertionError]
  }

}
