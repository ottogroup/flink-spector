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

package org.flinkspector.core.table;

import org.apache.flink.api.java.tuple.Tuple;
import org.flinkspector.core.KeyMatcherPair;
import org.flinkspector.core.table.tuple.TupleMatcher;
import org.hamcrest.Matcher;

/**
 * Enables the use of a {@link TupleMask} to map a {@link Tuple} to string keys.
 * And then use these keys in combination with hamcrest's {@link Matcher}s to define
 * expectations that query the output like a table.
 * <p/>
 * The class holds a list of matcher and string key pairs.
 * The user can add a set of pairs and in the next step define,
 * how many of these matchers have to be valid for a defined set of
 * records.
 * <p/>
 * <pre>
 * {@code
 * new AssertTuples<Tuple2<String,Integer>>("name","age")
 * 		.assertThat("age", greaterThan(21))
 * 		.assertThat("name", either(is("fritz")).or(is("peter")))
 * 		.anyOfThem().onEachRecord();
 * </pre>
 *
 * @param <T>
 */
public class AssertTuples<T extends Tuple> extends AssertOutput<T> {

	/**
	 * {@link TupleMask} used to map the keys to the inspected tuples.
	 */
	private final TupleMask<T> mask;

	/**
	 * Factory method accessing the default constructor.
	 *
	 * @param mask {@link TupleMask} to use.
	 * @param <T>  type of output
	 * @return new instance of {@link AssertTuples}
	 */
	public static <T extends Tuple> AssertTuples<T> fromMask(TupleMask<T> mask) {
		return new AssertTuples<T>(mask);
	}

	/**
	 * Default Constructor.
	 *
	 * @param mask {@link TupleMask} to use.
	 */
	public AssertTuples(TupleMask<T> mask) {
		super();
		this.mask = mask;
	}

	/**
	 * Constructor that provides a {@link TupleMask}
	 * from a set of string keys.
	 *
	 * @param first key
	 * @param rest of keys
	 */
	public AssertTuples(String first, String... rest) {
		this(new TupleMask<T>(first,rest));
	}

	/**
	 * Add a new assertion based on tuple to the list.
	 *
	 * @param key   of the field
	 * @param match matcher to use on the field
	 */
	public AssertTuples<T> assertThat(String key, Matcher match) {
		assertThat(new TupleMatcher<T>(KeyMatcherPair.of(key, match),mask));
		return this;
	}

	/**
	 * Add a {@link Matcher} to the list of assertions to verify.
	 * @param matcher testing the output records
	 */
	public AssertTuples<T> assertThatRecord(Matcher<? super T> matcher) {
		super.assertThat(matcher);
		return this;
	}
}
