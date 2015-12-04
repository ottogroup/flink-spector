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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.flinkspector.core.collection.ExpectedOutput;
import org.flinkspector.core.table.TupleMask;
import org.flinkspector.datastream.StreamTestBase;


/**
 * This example shows how to define input without time characteristics.
 * With the usage of {@link ExpectedOutput}.
 */
public class MapperTest extends StreamTestBase {

	/**
	 * DataStream transformation to test.
	 * Swaps the fields of a {@link Tuple2}
	 *
	 * @param stream input {@link DataStream}
	 * @return {@link DataStream}
	 */
	public static DataStream<Tuple2<String, Integer>> swap(DataStream<Tuple2<Integer, String>> stream) {
		return stream.map(new MapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<Integer, String> input) throws Exception {
				return input.swap();
			}
		});
	}

	/**
	 * JUnit Test
	 */
	@org.junit.Test
	public void testMap() {

		/*
		 * Define the input DataStream:
		 * Get a SourceBuilder with .createTestStreamWith(record).
		 * Add data records to it and retrieve a DataStreamSource
		 * by calling .complete().
		 */
		DataStream<Tuple2<Integer, String>> stream =
				createTestStreamWith(Tuple2.of(1, "test"))
						.emit(Tuple2.of(2, "foo"))
						.emit(Tuple2.of(3, "bar"))
						.close();


		/*
		 * Define the output you expect from the the transformation under test.
		 * Add the tuples you want to see with .expect(record).
		 */
		ExpectedOutput<Tuple2<String, Integer>> expectedOutput = new ExpectedOutput<Tuple2<String, Integer>>()
				.expect(Tuple2.of("test", 1))
				.expect(Tuple2.of("foo", 2));
		// refine your expectations by adding requirements
		expectedOutput.refine().noDuplicates().inOrder(strict).all();

		TupleMask<Tuple3<String,String,String>> mask = TupleMask.create("flip","flop","flup");

		/*
		 * Use assertStream to map DataStream to an OutputMatcher.
		 * ExpectedOutput extends OutputMatcher and thus can be used in this way.
		 * This means you're also able to combine ExpectedOutput with any
		 * OutputMatcher. E.g:
		 * assertStream(swap(stream), and(expectedOutput,outputWithSize(3))
		 * would additionally assert that the number of produced records is exactly 3.
		 */
		assertStream(swap(stream), expectedOutput);

	}
}
