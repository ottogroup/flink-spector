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

package org.flinkspector.core.quantify.assertions;

import org.apache.flink.api.java.tuple.Tuple;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at most one of the specified matchers.
 */
public class AtMost<T> extends WhileMatcherCombiner<T> {

	private final int n;

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link Matcher}
	 * @param n        number of expected matches
	 */
	public AtMost(Iterable<Matcher<? super T>> matchers, int n) {
		super(matchers);
		this.n = n;
	}

	protected Description describeCondition(Description description) {
		return description.appendText("at most ").appendValue(n);
	}

	@Override
	public String prefix() {
		return "at most ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches <= n;
	}

	@Factory
	public static <T> AtMost<T> atMost(Iterable<Matcher<? super T>> matchers,
													int n) {
		return new AtMost<T>(matchers, n);
	}
}
