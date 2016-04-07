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
package org.flinkspector.datastream.examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.flinkspector.core.quantify.MatchTuples;
import org.flinkspector.core.quantify.OutputMatcher;
import org.flinkspector.datastream.StreamTestBase;
import org.flinkspector.datastream.input.time.InWindow;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

//import to use the dsl of hamcrest:

/**
 * This example shows how to startWith test input with time characteristics.
 * And the usage of {@link MatchTuples} to build an {@link OutputMatcher}.
 * <p/>
 * To ensure test cases run in a few seconds the framework sets the time characteristic of the data flow, to
 * EventTime. The test source emitting the input, calculates and emits watermarks based on the timestamped input.
 */
public class WindowingTest extends StreamTestBase {

	/**
	 * Transformation to test.
	 * Builds 20 second windows and sums up the integer values.
	 *
	 * @param stream input {@link DataStream}
	 * @return {@link DataStream}
	 */
	public static DataStream<Tuple2<Integer, String>> window(DataStream<Tuple2<Integer, String>> stream) {
		return stream.timeWindowAll(Time.of(20, seconds)).sum(0);
	}

	@org.junit.Test
	public void testWindowing() {

		setParallelism(2);

		/*
		 * Define the input DataStream:
		 * Get a EventTimeSourceBuilder with, .createTimedTestStreamWith(record).
		 * Add data records to it and retrieve a DataStreamSource
		 * by calling .close().
		 *
		 * Note: The before and after keywords define the time span !between! the previous
		 * record and the current record.
		 */
		DataStream<Tuple2<Integer, String>> testStream =
				createTimedTestStreamWith(Tuple2.of(1, "fritz"))
						.emit(Tuple2.of(2, "fritz"))
						//it's possible to generate unsorted input
						.emit(Tuple2.of(2, "fritz"))
						//emit the tuple multiple times, with the time span between:
						.emit(Tuple2.of(1, "peter"), InWindow.to(20,seconds),times(2))
						.close();

		/*
		 * Creates an OutputMatcher using MatchTuples.
		 * MatchTuples builds an OutputMatcher working on Tuples.
		 * You assign String identifiers to your Tuple,
		 * and add hamcrest matchers testing the values.
		 */
		OutputMatcher<Tuple2<Integer, String>> matcher =
				//name the values in your tuple with keys:
				new MatchTuples<Tuple2<Integer, String>>("value", "name")
						//add an assertion using a value and hamcrest matchers
						.assertThat("value", is(6))
						.assertThat("name", either(is("fritz")).or(is("peter")))
						//express how many matchers must return true for your test to pass:
						//define how many records need to fulfill the
						.onEachRecord();

		/*
		 * Use assertStream to map DataStream to an OutputMatcher.
		 * You're also able to combine OutputMatchers with any
		 * OutputMatcher. E.g:
		 * assertStream(swap(stream), and(matcher, outputWithSize(greaterThan(4))
		 * would additionally assert that the number of produced records is exactly 3.
		 */
		assertStream(window(testStream), matcher);

	}

}
