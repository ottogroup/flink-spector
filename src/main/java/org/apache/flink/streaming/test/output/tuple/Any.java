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
import org.hamcrest.Factory;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at least one of the specified matchers.
 */
public class Any<T extends Tuple> extends UntilTuple<T> {

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 */
	public Any(Iterable<KeyMatcherPair> matchers,
			TupleMask<T> table) {
		super(matchers, table);
	}

	@Override
	public String prefix() {
		return "any of";
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == 1;
	}

	@Factory
	public static <T extends Tuple> Any<T> any(Iterable<KeyMatcherPair> matchers,
											TupleMask<T> table) {
		return new Any<T>(matchers, table);
	}
}
