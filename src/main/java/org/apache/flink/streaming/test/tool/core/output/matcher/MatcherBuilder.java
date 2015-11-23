/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.core.output.matcher;

import org.apache.flink.streaming.test.tool.core.assertion.OutputMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

/**
 * Wrapper for the scala {@link org.apache.flink.streaming.test.tool.matcher.ListMatcherBuilder}
 * @param <T>
 */
public class MatcherBuilder<T> extends OutputMatcher<T> {

	private org.apache.flink.streaming.test.tool.matcher.ListMatcherBuilder<T> builder;
	private List<T> right;

	public MatcherBuilder(List<T> right) {
		this.right = right;
		builder = new org.apache.flink.streaming.test.tool.matcher.ListMatcherBuilder<>(right);
	}

	/**
	 * Tests whether the output contains only the expected records
	 */
	public MatcherBuilder<T> only() {
		builder.only();
		return this;
	}

	/**
	 * Tests whether the output contains no duplicates in reference
	 * to the expected output
	 */
	public MatcherBuilder<T> noDuplicates() {
		builder.noDuplicates();
		return this;
	}

	/**
	 * Provides a {@link OrderMatcher} to verify the order of
	 * elements in the output
	 */
	public OrderMatcher<T> inOrder() {
		return new OrderMatcher<T>(builder);
	}

	@Override
	public void describeTo(Description description) {
		builder.describeTo(description);
	}

	@Override
	protected boolean matchesSafely(Iterable<T> item) {
		return builder.validate(item);
	}
}
