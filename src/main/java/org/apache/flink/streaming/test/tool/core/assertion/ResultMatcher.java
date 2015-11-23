/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.core.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.assertion.result.RecordsMatchers;
import org.hamcrest.Matcher;

/**
 * Takes a {@link Matcher} working on a {@link Tuple}
 * and converts it into a {@link OutputMatcher} matching an iterable of tuples.
 * Provides a set of methods to define the number of matches necessary to
 * fulfill the expectation.
 * @param <T>
 */
public class ResultMatcher<T extends Tuple> {

	/**
	 * wrapped {@link Matcher}
	 */
	private final Matcher<T> matcher;

	/**
	 * Default constructor.
	 *
	 * @param matcher
	 */
	public ResultMatcher(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	/**
	 * Creates a {@link OutputMatcher} matching when at least one
	 * inspected tuple returns a positive match.
	 *
	 * @return {@link OutputMatcher}
	 */
	public OutputMatcher<T> onAnyRecord() {
		return OutputMatcher.create(RecordsMatchers.any(matcher));
	}

	/**
	 * Creates a {@link OutputMatcher} matching when all
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcher}
	 */
	public OutputMatcher<T> onEachRecord() {
		return OutputMatcher.create(RecordsMatchers.each(matcher));
	}

	/**
	 * Creates a {@link OutputMatcher} matching when exactly one inspected tuple
	 * return a positive match.
	 *
	 * @return {@link OutputMatcher}
	 */
	public OutputMatcher<T> onOneRecord() {
		return OutputMatcher.create(RecordsMatchers.each(matcher));
	}

	/**
	 * Creates a {@link OutputMatcher} matching when no inspected tuple
	 * return a positive match.
	 *
	 * @return {@link OutputMatcher}
	 */
	public OutputMatcher<T> onNoRecord() {
		return OutputMatcher.create(RecordsMatchers.none(matcher));
	}

	/**
	 * Creates a {@link OutputMatcher} matching when a exact number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcher}
	 */
	public OutputMatcher<T> onExactlyNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.exactly(matcher, n));
	}

	/**
	 * Creates a {@link OutputMatcher} matching when at least a number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcher}
	 */
	public OutputMatcher<T> onAtLeastNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.atLeast(matcher,n));
	}

	/**
	 * Creates a {@link OutputMatcher} matching when at most a number of
	 * inspected tuples return a positive match.
	 *
	 * @return {@link OutputMatcher}
	 */
	public OutputMatcher<T> onAtMostNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.atMost(matcher,n));
	}


}