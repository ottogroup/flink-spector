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

package org.apache.flink.core.set;

import org.apache.flink.core.matcher.ListMatcherBuilder;
import org.apache.flink.core.table.OutputMatcher;
import org.hamcrest.Description;

import java.util.List;

/**
 * Wrapper for the scala {@link ListMatcherBuilder}
 * @param <T>
 */
public class MatcherBuilder<T> extends OutputMatcher<T> {

	private ListMatcherBuilder<T> builder;
	private List<T> right;

	public MatcherBuilder(List<T> right) {
		this.right = right;
		builder = new ListMatcherBuilder<>(right);
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
	public FromListMatcher inOrder(Order order) {
		if(order == Order.STRICT) {
			return new SeriesMatcher<T>(builder);
		}
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

	public enum Order {
		STRICT,NONSTRICT
	}
}
