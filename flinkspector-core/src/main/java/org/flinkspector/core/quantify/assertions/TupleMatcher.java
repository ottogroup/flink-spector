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
import org.flinkspector.core.quantify.TupleMap;
import org.flinkspector.core.quantify.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Wraps a {@link Matcher} to verify a single value in a {@link Tuple}.
 * Uses a {@link TupleMask} on the tuple and key to identify the value.
 * @param <T>
 */
public class TupleMatcher<T extends Tuple> extends TypeSafeDiagnosingMatcher<T> {

	private final String key;
	private final Matcher matcher;
	private final TupleMask<T> mask;

	public TupleMatcher(String key, Matcher matcher, TupleMask<T> mask) {
		this.mask = mask;
		this.key = key;
		this.matcher = matcher;
	}

	@Override
	protected boolean matchesSafely(T item, Description mismatchDescription) {
		TupleMap tupleMap = mask.apply(item);

		if (!matcher.matches(tupleMap.get(key))) {
			mismatchDescription
					.appendText("[" + key + "] ")
					.appendDescriptionOf(matcher)
					.appendText(", ");
			matcher.describeMismatch(tupleMap.get(key), mismatchDescription);
			return false;
		}
		return true;
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("[" + key + "] ");
		description.appendDescriptionOf(matcher);
	}
}
