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

package org.flinkspector.core.table.result;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher}s that is successful if at least n
 * items in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class AtLeast<T> extends UntilRecord<T> {

	private final int n;

	/**
	 * Default Constructor
	 *
	 * @param matcher to apply to the {@link Iterable}
	 * @param n       number of expected positive matches
	 */
	public AtLeast(Matcher<T> matcher, int n) {
		super(matcher);
		this.n = n;
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("at least ").appendValue(n);
	}

	@Override
	protected boolean validWhen(int numMatches, int possibleMatches) {
		return numMatches == n;
	}

	@Override
	public String prefix() {
		return "at least n records ";
	}

	@Factory
	public static <T> AtLeast<T> atLeast(Matcher<T> matcher, int n) {
		return new AtLeast<>(matcher, n);
	}

}