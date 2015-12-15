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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

abstract public class WhileMatcherCombiner<T> extends TypeSafeDiagnosingMatcher<T> {
	//TODO play with description

	private final Iterable<Matcher<? super T>> matchers;

	public WhileMatcherCombiner(Iterable<Matcher<? super T>> matchers) {
		this.matchers = matchers;
	}

	@Override
	public boolean matchesSafely(T item, Description mismatch) {
		int matches = 0;
		Description mismatches = new StringDescription();
		mismatch.appendText("\n          ");
		for (Matcher<? super T> matcher : matchers) {
			if (!matcher.matches(item)) {
				matcher.describeMismatch(item, mismatches);
				mismatches.appendText("; ");
			} else {
				matches++;
			// Check exit condition if not valid exit matcher.
				if (!validWhile(matches)) {
					describeMismatch(matches, true, mismatch, mismatches);
					return false;
				}
			}
		}
		describeMismatch(matches, false, mismatch, mismatches);
		return validAfter(matches);
	}

	private void describeMismatch(int numMatches,
								  Boolean tooMany,
								  Description mismatch,
								  Description mismatches) {


		if (tooMany) {
			mismatch.appendText("expected matches in block ");
			describeCondition(mismatch);
			mismatch.appendText(", was ")
					.appendValue(numMatches)
					.appendText(" ");
		} else {
			mismatch.appendText(mismatches.toString());
		}

	}

	@Override
	public void describeTo(Description description) {
		description.appendText(prefix());
		description.appendText("(");
		for (Matcher m : this.matchers) {
			if(!description.toString().endsWith("(")) {
				description.appendText("; ");
			}
			description.appendDescriptionOf(m);
		}
		description.appendText(")");
	}

	protected abstract Description describeCondition(Description description);

	public abstract boolean validWhile(int matches);

	public abstract String prefix();

	public boolean validAfter(int matches){
		return true;
	}

}
