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

import org.flinkspector.CoreSpec
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.TypeExtractor

class SerializeUtilSpec extends CoreSpec {

  class Person {
    var age: Integer = _
    var name: String = _

  }

  trait SerializeUtilCase {
    val config = new ExecutionConfig()
    val typeInfo : TypeInformation[String] = TypeExtractor.getForObject("test")
    val serializer = typeInfo.createSerializer(config)
  }

  "The util" should "serialize and deserialize a String" in new SerializeUtilCase{
    val string = "test-string"
    val bytes = SerializeUtil.serialize(string,serializer)

    SerializeUtil.deserialize(bytes,serializer) should equal(string)
  }

  "The util" should "serialize and deserialize a [[TypeSerializer]]" in new SerializeUtilCase {
    val bytes = SerializeUtil.serialize(serializer)
    SerializeUtil.deserialize(bytes).asInstanceOf[TypeSerializer[String]] should equal(serializer)
  }

  "The util" should "serialize and deserialize a [[Pair]]" in {
    val tuple = ("right","left")
    val serializer = TypeExtractor
      .getForObject(tuple)
      .createSerializer(new ExecutionConfig)
    val bytes = SerializeUtil.serialize(tuple,serializer)
    SerializeUtil.deserialize(bytes,serializer) should equal(tuple)
  }



}
