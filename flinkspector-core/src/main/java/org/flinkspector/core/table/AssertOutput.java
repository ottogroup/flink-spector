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
import org.flinkspector.core.KeyMatcherPair;
import org.flinkspector.core.table.tuple.MatcherCombiner;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

/**
 * Lets you build a list of assertion and define how often they should match your output
 * <pre>
 * {@code
 * new AssertOutput<Integer>("name","age")
 * 		.assertThat(greaterThan(21))
 * 		.assertThat(lessThan(11))
 * 		anyOfThem().onEachRecord();
 * </pre>
 * @param <T>
 */
public class AssertOutput<T> {

	/**
	 * List of {@link KeyMatcherPair} representing the assertions.
	 */
	private List<Matcher<? super T>> matchers = new ArrayList<>();

	/**
	 * Expect the record to fulfill at least one of the specified asserts.
	 *
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> anyOfThem() {
		return new ResultMatcher<>(MatcherCombiner.any(matchers));
	}

	/**
	 * Expect the record to fulfill exactly one of the specified asserts.
	 *
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> oneOfThem() {
		return new ResultMatcher<>(MatcherCombiner.one(matchers));
	}


	/**
	 * Add a {@link Matcher} to the list of assertions to verify.
	 * @param matcher testing the output records
	 */
	public AssertOutput<T> assertThat(Matcher<? super T> matcher) {
		matchers.add(matcher);
		return this;
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
		return new ResultMatcher<>(MatcherCombiner.exactly(matchers, n));
	}

	/**
	 * Expect the record to fulfill at least a number of the specified asserts.
	 *
	 * @param n number of minimum matches.
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> atLeastNOfThem(int n) {
		return new ResultMatcher<>(MatcherCombiner.atLeast(matchers, n));
	}

	/**
	 * Expect record to fulfill at most a number of the specified asserts.
	 *
	 * @param n number of maximum matches.
	 * @return {@link ResultMatcher} to define the number of matching records in
	 * the output.
	 */
	public ResultMatcher<T> atMostNOfThem(int n) {
		return new ResultMatcher<>(MatcherCombiner.atMost(matchers, n));
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when at least one
	 * inspected tuple returns a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onAnyRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(MatcherCombiner.each(matchers));
		return OutputMatcherFactory.create(matcher.onAnyRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when all
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onEachRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(MatcherCombiner.each(matchers));
		return OutputMatcherFactory.create(matcher.onEachRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when exactly one inspected tuple
	 * return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onOneRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(MatcherCombiner.each(matchers));
		return OutputMatcherFactory.create(matcher.onOneRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when no inspected tuple
	 * return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onNoRecord() {
		ResultMatcher<T> matcher = new ResultMatcher<>(MatcherCombiner.each(matchers));
		return OutputMatcherFactory.create(matcher.onNoRecord());
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when a exact number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onExactlyNRecords(int n) {
		ResultMatcher<T> matcher = new ResultMatcher<>(MatcherCombiner.each(matchers));
		return OutputMatcherFactory.create(matcher.onExactlyNRecords(n));
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when at least a number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onAtLeastNRecords(int n) {
		ResultMatcher<T> matcher = new ResultMatcher<>(MatcherCombiner.each(matchers));
		return OutputMatcherFactory.create(matcher.onAtLeastNRecords(n));
	}

	/**
	 * Creates a {@link OutputMatcherFactory} matching when at most a number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcherFactory}
	 */
	public OutputMatcher<T> onAtMostNRecords(int n) {
		ResultMatcher<T> matcher = new ResultMatcher<>(MatcherCombiner.each(matchers));
		return OutputMatcherFactory.create(matcher.onAtMostNRecords(n));
	}

	@VisibleForTesting
	List<Matcher<? super T>> getMatchers() {
		return matchers;
	}


}
