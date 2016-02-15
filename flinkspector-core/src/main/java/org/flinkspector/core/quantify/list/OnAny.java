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

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher}s that is successful if at least one
 * item in the examined {@link Iterable} is a positive match.
 * @param <T>
 */
public class OnAny<T> extends UntilList<T> {

	/**
	 * Default Constructor
	 * @param matcher to apply to the {@link Iterable}
	 */
	public OnAny(Matcher<T> matcher) {
		super(matcher);
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("at least ").appendValue(1);
	}

	@Override
	protected boolean validWhen(int matches, int possibleMatches) {
		return matches == 1;
	}

	@Override
	public String prefix() {
		return "any record ";
	}

	@Factory
	public static <T> OnAny<T> any(Matcher<T> matcher) {
		return new OnAny<>(matcher);
	}

}
