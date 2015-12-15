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
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at least one of the specified matchers.
 */
public class Any<T> extends UntilMatcherCombiner<T> {

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link Matcher}
	 */
	public Any(Iterable<Matcher<? super T>> matchers) {
		super(matchers);
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
	public static <T> Any<T> any(Iterable<Matcher<? super T>> matchers) {
		return new Any<T>(matchers);
	}
}
