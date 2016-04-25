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

package org.flinkspector.core.quantify;

import org.flinkspector.core.quantify.records.OutputWithSize;
import org.hamcrest.core.AllOf;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsNot;

/**
 * Wrapper around default matchers, to combine matchers, from hamcrest.
 */
public class OutputMatchers {

	/**
	 * Creates a matcher that matches if the examined object matches <b>ALL</b> of the specified matchers.
	 */
	@SafeVarargs
	public static <T> OutputMatcher<T> allOf(OutputMatcher<T>... matchers) {
		return OutputMatcherFactory.create(AllOf.allOf(matchers));
	}


	/**
	 * Creates a matcher that matches if the examined object matches <b>ANY</b> of the specified matchers.
	 */
	@SafeVarargs
	public static <T> OutputMatcher<T> anyOf(OutputMatcher<T>... matchers) {
		return OutputMatcherFactory.create(AnyOf.<Iterable<T>>anyOf(matchers));
	}

	/**
	 * Creates a matcher that wraps an existing matcher, but inverts the logic by which
	 * it will match.
	 *
	 * @param matcher
	 *     the matcher whose sense should be inverted
	 */
	public static <T> OutputMatcher<T> not(OutputMatcher<T> matcher) {
		return OutputMatcherFactory.create(IsNot.not(matcher));
	}

	/**
	 * Creates a matcher that matches when both of the specified matchers match the examined object.
	 * <p/>
	 * For example:
	 * <pre>assertThat("fab", both(containsString("a")).and(containsString("b")))</pre>
	 */
	public static <LHS> org.hamcrest.core.CombinableMatcher.CombinableBothMatcher<Iterable<LHS>> both(OutputMatcher<LHS> matcher) {
		return org.hamcrest.core.CombinableMatcher.both(matcher);
	}

	/**
	 * Creates a matcher that matches when either of the specified matchers match the examined object.
	 */
	public static <LHS> org.hamcrest.core.CombinableMatcher.CombinableEitherMatcher<Iterable<LHS>> either(OutputMatcher<LHS> matcher) {
		return org.hamcrest.core.CombinableMatcher.either(matcher);
	}

	/**
	 * Creates a matcher for output that matches when the <code>size</code> of the output
	 * satisfies the specified matcher.
	 *
	 * @param sizeMatcher
	 *     a matcher for the length of an examined array
	 */
	public static <E> OutputMatcher<E> outputWithSize(org.hamcrest.Matcher<? super java.lang.Integer> sizeMatcher) {
		return OutputMatcherFactory.create(OutputWithSize.<E>outputWithSize(sizeMatcher));
	}

	/**
	 * Creates a matcher for output that matches when the <code>length</code> of the output
	 * equals the specified <code>size</code>.
	 *
	 * @param size
	 *     the length that an examined array must have for a positive match
	 */
	public static <E> OutputMatcher<E> outputWithSize(int size) {
		return OutputMatcherFactory.create(OutputWithSize.<E>outputWithSize(size));
	}

}
