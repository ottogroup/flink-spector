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


import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if exactly one
 * item in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class One<T> extends WhileMatcherCombiner<T> {

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link Matcher}
	 */
	public One(Iterable<Matcher<? super T>> matchers) {
		super(matchers);
	}

	protected Description describeCondition(Description description) {
		return description.appendText("exactly ").appendValue(1);
	}

	@Override
	public String prefix() {
		return "one of ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches <= 1;
	}

	@Override
	public boolean validAfter(int matches) {
		return matches == 1;
	}

	@Factory
	public static <T> One<T> one(Iterable<Matcher<? super T>> matchers) {
		return new One<T>(matchers);
	}
}
