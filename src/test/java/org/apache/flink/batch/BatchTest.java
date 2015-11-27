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

package org.apache.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.set.ExpectedOutput;
import org.apache.flink.core.input.Input;
import org.apache.flink.core.input.InputBuilder;
import org.apache.flink.core.table.AssertBlock;
import org.apache.flink.core.table.OutputMatcher;
import org.apache.flink.core.trigger.FinishAtCount;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.apache.flink.core.table.OutputMatchers.anyOf;

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
		Input<Tuple2<Integer, String>> input = InputBuilder
				.create(Tuple2.of(1, "test"))
				.emit(Tuple2.of(2, "why"))
				.emit(Tuple2.of(3, "not"))
				.emit(Tuple2.of(4, "batch?"));

		DataSet<Tuple2<Integer, String>> testDataSet = createTestDataSet(input);

		ExpectedOutput<Tuple2<String, Integer>> output = ExpectedOutput
				.create(Tuple2.of("test", 1))
				.expect(Tuple2.of("why", 2))
				.expect(Tuple2.of("not", 3));
		output.refine().only();

		OutputMatcher<Tuple2<String, Integer>> matcher =
				new AssertBlock<Tuple2<String, Integer>>("name", "value")
						.assertThat("name", isA(String.class))
						.assertThat("value", lessThan(5))
						.onEachRecord();

		assertDataSet(swap(testDataSet), anyOf(output, matcher), FinishAtCount.of(3));
	}

}
