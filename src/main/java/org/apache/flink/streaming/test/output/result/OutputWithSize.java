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

package org.apache.flink.streaming.test.output.result;

import com.google.common.collect.Iterables;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.core.IsEqual;

/**
 * Provides a matcher for output that matches when the <code>size</code> of the output
 * satisfies the specified matcher.
 * @param <T>
 */
public class OutputWithSize<T> extends TypeSafeDiagnosingMatcher<Iterable<T>> {

	Matcher<? super Integer> sizeMatcher;

	public OutputWithSize(Matcher<? super Integer> sizeMatcher) {
		this.sizeMatcher = sizeMatcher;
	}

	@Override
	protected boolean matchesSafely(Iterable<T> item, Description mismatchDescription) {
		int size = Iterables.size(item);
		boolean matches = sizeMatcher.matches(size);
		if(!matches) {
			sizeMatcher.describeMismatch(size,mismatchDescription);
		}
		return matches;
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("output with size: ");
		sizeMatcher.describeTo(description);
	}

	public static <T> OutputWithSize<T> outputWithSize(Matcher<? super Integer> sizeMatcher) {
		return new OutputWithSize<T>(sizeMatcher);
	}

	public static <T> OutputWithSize<T> outputWithSize(int size) {
		return  OutputWithSize.<T>outputWithSize(IsEqual.equalTo(size));
	}
}
