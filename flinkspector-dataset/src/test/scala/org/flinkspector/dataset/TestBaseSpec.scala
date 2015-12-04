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

package org.flinkspector.dataset

import org.apache.flink.api.java.DataSet
import org.flinkspector.core.collection.ExpectedRecords

import scala.collection.JavaConversions._

class TestBaseSpec extends CoreSpec{

  it should "run a basic test" in {
    val base = new TestBase
    base.initialize()

    val coll : java.util.Collection[Int] = List(1,2,3,4)
    val dataSet : DataSet[Int] = base.createTestDataSet(coll)
    val matcher = ExpectedRecords.create(coll)

    base.assertDataSet(dataSet,matcher)
    base.executeTest()
  }

  it should "fail a basic test" in {
    val base = new TestBase
    base.initialize()

    val coll : java.util.Collection[Int] = List(1,2,3,4)
    val dataSet : DataSet[Int] = base.createTestDataSet(List(1,2,3))
    val matcher = ExpectedRecords.create(coll)

    base.assertDataSet(dataSet,matcher)
    an [AssertionError] shouldBe thrownBy(base.executeTest())
  }


  it should "run a basic test with two sinks" in {
    val base = new TestBase
    base.initialize()

    val coll : java.util.Collection[Int] = List(1,2,3,4)
    val dataSet1 : DataSet[Int] = base.createTestDataSet(coll)
    val dataSet2 : DataSet[Int] = base.createTestDataSet(coll)
    val matcher = ExpectedRecords.create(coll)

    base.assertDataSet(dataSet1,matcher)
    base.assertDataSet(dataSet2,matcher)
    base.executeTest()
  }

  it should "fail a basic test with two sinks" in {
    val base = new TestBase
    base.initialize()

    val coll : java.util.Collection[Int] = List(1,2,3,4)
    val dataSet1 : DataSet[Int] = base.createTestDataSet(coll)
    val dataSet2 : DataSet[Int] = base.createTestDataSet(List(1,2,3))
    val matcher = ExpectedRecords.create(coll)

    base.assertDataSet(dataSet1,matcher)
    base.assertDataSet(dataSet2,matcher)
    an [AssertionError] shouldBe thrownBy (base.executeTest())
  }


}
