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

package org.flinkspector.datastream.input

import java.util
import java.util.concurrent.TimeUnit

import org.flinkspector.CoreSpec
import org.flinkspector.core.runtime.SimpleOutputVerifier
import org.flinkspector.datastream.DataStreamTestEnvironment
import org.flinkspector.datastream.input.time.After

class EventTimeSourceBuilderSpec extends CoreSpec {

  class Verifier[T](list: List[T]) extends SimpleOutputVerifier[T] {
    override def verify(output: util.List[T]): Unit =
      output should contain theSameElementsAs list
  }

  "The source builder" should "startWith a working source" in {
    val env = DataStreamTestEnvironment.createTestEnvironment(1)

    val source = new EventTimeSourceBuilder[Int](env, 1)
      .emit(2, After.period(1, TimeUnit.SECONDS))
      .emit(3, After.period(1, TimeUnit.SECONDS))
      .emit(4, After.period(1, TimeUnit.SECONDS))
      .close()

    source.addSink {
      env.createTestSink(new Verifier[Int](List(1, 2, 3, 4)))
    }

    env.executeTest()
  }

}
