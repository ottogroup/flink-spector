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

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple;
import org.flinkspector.core.KeyMatcherPair;
import org.flinkspector.core.table.tuple.TupleMapMatchers;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

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
 * new AssertBlock<Tuple2<String,Integer>>("name","age")
 * 		.assertThat("age", greaterThan(21))
 * 		.assertThat("name", either(is("fritz")).or(is("peter")))
 * 		.eachOfThem().onAnyRecord();
 * </pre>
 *
 * @param <T>
 */
public class AssertBlock<T extends Tuple> {

	/**
	 * {@link TupleMask} used to map the keys to the inspected tuples.
	 */
	private final TupleMask<T> mask;

	/**
	 * List of {@link KeyMatcherPair} representing the assertions.
	 */
	private List<KeyMatcherPair> matchers = new ArrayList<>();

	/**
	 * Default Constructor.
	 *
	 * @param mask {@link TupleMask} to use.
	 */
	public AssertBlock(TupleMask<T> mask) {
		this.mask = mask;
	}

	/**
	 * Constructor that provides a {@link TupleMask}
	 * from a set of string keys.
	 *
	 * @param first key
	 * @param rest of keys
	 */
	public AssertBlock(String first, String... rest) {
		this(new TupleMask<T>(first,rest));
	}

	/**
	 * Factory method accessing the default constructor.
	 *
	 * @param mask {@link TupleMask} to use.
	 * @param <T>  type of output
	 * @return new instance of {@link AssertBlock}
	 */
	public static <T extends Tuple> AssertBlock<T> fromMask(TupleMask<T> mask) {
		return new AssertBlock<T>(mask);
	}

	/**
	 * Add a new assertion to the list.
	 *
	 * @param key   of the field
	 * @param match matcher to use on the field
	 */
	public AssertBlock<T> assertThat(String key, Matcher match) {
		matchers.add(KeyMatcherPair.of(key, match));
		return this;
	}

	/**
	 * Expect the record to fulfill at least one of the specified asserts.
	 *
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> anyOfThem() {
		return new ResultMatcher<>(TupleMapMatchers.any(matchers, mask));
	}

	/**
	 * Expect the record to fulfill exactly one of the specified asserts.
	 *
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> oneOfThem() {
		return new ResultMatcher<>(TupleMapMatchers.one(matchers, mask));
	}

//	/**
//	 * Expect the record to fulfill all of the specified asserts.
//	 *
//	 * @return {@link ResultMatcher} to define the number of matching records in
//	 * the output.
//	 */
//	public ResultMatcher<T> eachOfThem() {
//		return new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
//	}

	/**
	 * Expect the record to fulfill at least exactly n of the specified asserts.
	 *
	 * @param n number of expected positive matches.
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> exactlyNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.exactly(matchers, mask, n));
	}

	/**
	 * Expect the record to fulfill at least a number of the specified asserts.
	 *
	 * @param n number of minimum matches.
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> atLeastNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.atLeast(matchers, mask, n));
	}

	/**
	 * Expect record to fulfill at most a number of the specified asserts.
	 *
	 * @param n number of maximum matches.
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> atMostNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.atMost(matchers, mask, n));
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when at least one
	 * inspected tuple returns a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onAnyRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
		return OutputMatcherFactory.create(matcher.onAnyRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when all
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onEachRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
		return OutputMatcherFactory.create(matcher.onEachRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when exactly one inspected tuple
	 * return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onOneRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
		return OutputMatcherFactory.create(matcher.onOneRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when no inspected tuple
	 * return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onNoRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
		return OutputMatcherFactory.create(matcher.onNoRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when a exact number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onExactlyNRecords(int n) {
		ResultMatcher<T> matcher = new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
		return OutputMatcherFactory.create(matcher.onExactlyNRecords(n));
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when at least a number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onAtLeastNRecords(int n) {
		ResultMatcher<T> matcher = new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
		return OutputMatcherFactory.create(matcher.onAtLeastNRecords(n));
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when at most a number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onAtMostNRecords(int n) {
		ResultMatcher<T> matcher = new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
		return OutputMatcherFactory.create(matcher.onAtMostNRecords(n));
	}

	@VisibleForTesting
	List<KeyMatcherPair> getKeyMatcherPairs() {
		return matchers;
	}


}
