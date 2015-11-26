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

package org.apache.flink.streaming.test.output.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.core.KeyMatcherPair;
import org.apache.flink.streaming.test.output.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Factory;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at least a number of the specified matchers.
 */
public class AtLeast<T extends Tuple> extends UntilTuple<T> {

	private final int n;

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 * @param n        number of expected matches
	 */
	public AtLeast(Iterable<KeyMatcherPair> matchers, TupleMask<T> mask, int n) {
		super(matchers, mask);
		this.n = n;
	}

	public Description describeCondition(Description description) {
		return description.appendText("at least ").appendValue(n);
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == n;
	}

	@Override
	public String prefix() {
		return "at least ";
	}

	@Factory
	public static <T extends Tuple> AtLeast<T> atLeast(Iterable<KeyMatcherPair> matchers,
													TupleMask<T> mask,
													int n) {
		return new AtLeast<>(matchers, mask, n);
	}
}
