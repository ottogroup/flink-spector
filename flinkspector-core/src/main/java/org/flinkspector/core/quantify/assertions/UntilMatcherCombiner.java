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

import com.google.common.collect.Iterables;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

public abstract class UntilMatcherCombiner<T> extends TypeSafeDiagnosingMatcher<T> {

	private final Iterable<Matcher<? super T>> matchers;

	public UntilMatcherCombiner(Iterable<Matcher<? super T>> matchers) {
		this.matchers = matchers;
	}

	@Override
	public boolean matchesSafely(T object, Description mismatch) {
		int matches = 0;
		int possibleMatches = Iterables.size(matchers);

		for (Matcher<? super T> matcher : matchers) {
			if (!matcher.matches(object)) {
				if(!mismatch.toString().endsWith("but: ")) {
					mismatch.appendText("\n          ");
				}
				matcher.describeMismatch(object, mismatch);
			} else {
				matches++;
				if (validWhen(matches,possibleMatches)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void describeTo(Description description) {
		List<Matcher> matchers = new ArrayList<>();
		description.appendText(prefix());
		description.appendText(" ( ");
		for (Matcher m : this.matchers) {
			if(!description.toString().endsWith("( ")) {
				description.appendText("; ");
			}
			description.appendDescriptionOf(m);
		}
		description.appendText(") ");
	}

	public abstract String prefix();

	public abstract boolean validWhen(int matches, int possibleMatches);
}
