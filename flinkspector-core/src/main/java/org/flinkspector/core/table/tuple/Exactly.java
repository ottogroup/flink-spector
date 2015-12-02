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

package org.flinkspector.core.table.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.flinkspector.core.KeyMatcherPair;
import org.flinkspector.core.table.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if exactly n
 * items in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class Exactly<T extends Tuple> extends WhileTuple<T> {

	private final int n;

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 * @param n        number of expected matches
	 */
	public Exactly(Iterable<KeyMatcherPair> matchers, TupleMask<T> mask, int n) {
		super(matchers, mask);
		this.n = n;
	}

	protected Description describeCondition(Description description) {
		return description.appendText("exactly ").appendValue(n);
	}

	@Override
	public String prefix() {
		return "exactly ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches <= n;
	}

	@Override
	public boolean validAfter(int matches) {
		return matches == n;
	}

	@Factory
	public static <T extends Tuple> Exactly<T> exactly(Iterable<KeyMatcherPair> matchers,
													TupleMask<T> mask,
													int n) {
		return new Exactly<T>(matchers, mask, n);
	}
}


