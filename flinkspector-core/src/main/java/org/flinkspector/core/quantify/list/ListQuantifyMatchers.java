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

package org.flinkspector.core.quantify.list;

import org.hamcrest.Matcher;
import org.hamcrest.core.IsNot;

/**
 * Offers a set of factory methods to startWith
 * {@link Matcher}s that takes an {@link Iterable}
 * and excepts a certain number of positive matches.
 */
public class ListQuantifyMatchers {

	/**
	 * Creates a {@link Matcher} that is successful if at least one
	 * item in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> any(Matcher<T> matcher) {
		return OnAny.<T>any(matcher);
	}

	/**
	 * Creates a {@link Matcher} that is successful if each
	 * item in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> each(Matcher<T> matcher) {
		return OnEach.<T>each(matcher);
	}

	/**
	 * Creates a {@link Matcher} that is successful if exactly one
	 * item in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static  <T> Matcher<Iterable<T>> one(Matcher<T> matcher) {
		return OnOne.<T>one(matcher);
	}

	/**
	 * Creates a {@link Matcher} that is successful if at least a number of
	 * items in the examined {@link Iterable} is a positive  match.
	 * @param matcher to apply to the list
	 * @param n number of positive matches
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> atLeast(Matcher<T> matcher, int n) {
		return OnAtLeast.<T>atLeast(matcher,n);
	}

	/**
	 * Creates a {@link Matcher} that is successful if at most a number of
	 * items in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param n number of positive matches
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> atMost(Matcher<T> matcher, int n) {
		return OnAtMost.<T>atMost(matcher,n);
	}

	/**
	 * Creates a {@link Matcher} that is successful if an exact number of
	 * items in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param n number of positive matches
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> exactly(Matcher<T> matcher, int n) {
		return OnExactly.<T>exactly(matcher,n);
	}

	/**
	 * Creates a {@link Matcher} that is successful if an exact number of
	 * items in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> none(Matcher<T> matcher) {
		return IsNot.not(any(matcher));
	}



}
