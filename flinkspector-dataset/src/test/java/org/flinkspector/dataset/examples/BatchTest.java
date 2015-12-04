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

package org.flinkspector.dataset.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.flinkspector.core.collection.ExpectedOutput;
import org.flinkspector.core.table.AssertBlock;
import org.flinkspector.core.table.OutputMatcher;
import org.flinkspector.core.trigger.FinishAtCount;
import org.flinkspector.dataset.TestBase;
import org.junit.Test;

import static org.flinkspector.core.table.OutputMatchers.anyOf;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThan;

public class BatchTest extends TestBase {

	private DataSet<Tuple2<String, Integer>> swap(DataSet<Tuple2<Integer, String>> set) {
		return set.map(new MapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<Integer, String> value) throws Exception {
				return value.swap();
			}
		});
	}


	@Test
	public void testMap() throws Throwable {

		/*
		 * Define the input DataSet:
		 * Get a DataSetBuilder with .createTestStreamWith(record).
		 * Add data records to it and retrieve a DataSet,
		 * by calling .complete().
		 */
		DataSet<Tuple2<Integer, String>> testDataSet =
				createTestDataSetWith(Tuple2.of(1,"test"))
						.emit(Tuple2.of(2, "why"))
						.emit(Tuple2.of(3, "not"))
						.emit(Tuple2.of(4, "batch?"))
						.close();

		/*
		 * Define the output you expect from the the transformation under test.
		 * Add the tuples you want to see with .expect(record).
		 */
		ExpectedOutput<Tuple2<String, Integer>> output = ExpectedOutput
				.create(Tuple2.of("test", 1))
				.expect(Tuple2.of("why", 2))
				.expect(Tuple2.of("not", 3));
		// refine your expectations by adding requirements
		output.refine().only().inOrder(strict);

		/*
		 * Creates an OutputMatcher using AssertBlock.
		 * AssertBlock builds an OutputMatcher working on Tuples.
		 * You assign String identifiers to your Tuple,
		 * and add hamcrest matchers testing the values.
		 */
		OutputMatcher<Tuple2<String, Integer>> matcher =
				//name the values in your tuple with keys:
				new AssertBlock<Tuple2<String, Integer>>("name", "value")
						//add an assertion using a value and hamcrest matchers
						.assertThat("name", isA(String.class))
						.assertThat("value", lessThan(5))
						//express how many matchers must return true for your test to pass:
						.anyOfThem()
						//define how many records need to fulfill the
						.onEachRecord();

		/*
		 * Use assertDataSet to map DataSet to an OutputMatcher.
		 * ExpectedOutput extends OutputMatcher and thus can be used in this way.
		 * Combine the created matchers with anyOf(), implicating that at least one of
		 * the matchers must be positive.
		 */
		assertDataSet(swap(testDataSet), anyOf(output, matcher), FinishAtCount.of(3));
	}

	private class Person {
	}
}
