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
package org.apache.flink.core.set

import org.apache.flink.core.runtime.OutputVerifier
import org.apache.flink.streaming.CoreSpec
import org.mockito.Mockito._

class StringToInt(verifier: OutputVerifier[Int])
  extends OutputTranslator[String,Int](verifier) {
  override def translate(record: String): Int = Integer.parseInt(record)
}

class OutputTranslatorSpec extends CoreSpec {

  "the translator" should "translate output" in {
    val verifier = mock[OutputVerifier[Int]]
    val translatedVerifier = new StringToInt(verifier)

    translatedVerifier.init()
    translatedVerifier.receive("1")
    translatedVerifier.receive("2")
    translatedVerifier.receive("3")
    translatedVerifier.receive("4")
    translatedVerifier.receive("5")
    translatedVerifier.finish()

    verify(verifier).init()
    verify(verifier).receive(1)
    verify(verifier).receive(2)
    verify(verifier).receive(3)
    verify(verifier).receive(4)
    verify(verifier).receive(5)
    verify(verifier).finish()
  }

}
