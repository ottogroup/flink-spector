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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * Matches an {@link Iterable} while a certain condition is met.
 * @param <T>
 */
public abstract class WhileRecord<T> extends TypeSafeDiagnosingMatcher<Iterable<T>> {

	private final Matcher<T> matcher;

	public WhileRecord(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	@Override
	public boolean matchesSafely(Iterable<T> objects, Description mismatch) {
		int numMatches = 0;
		int numMismatches = 0;
		Description mismatches = new StringDescription();
		int i = 0;
		for (T item : objects) {
			if (!matcher.matches(item)) {
				if(numMismatches < 10) {
					matcher.describeMismatch(item, mismatches);
					mismatches.appendText(" on record #" + (i + 1));
				}
				numMismatches++;
			} else {
				numMatches++;
			}
			if (!validWhile(numMatches, numMismatches)) {
				describeMismatch(numMatches, numMismatches, true, mismatch, mismatches);
				return false;
			}
			i++;
		}
		describeMismatch(numMatches, numMismatches, false, mismatch, mismatches);
		return validAfter(numMatches);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText(prefix());
		description.appendDescriptionOf(matcher);
	}

	private void describeMismatch(int numMatches,
								  int numMismatches,
								  Boolean tooMany,
								  Description mismatch,
								  Description mismatches) {
		mismatch.appendText("expected matching records to be ");
		describeCondition(mismatch);

		if (tooMany) {
			mismatch.appendText(", mismatches was ")
					.appendValue(numMismatches)
					.appendText(" because");

		} else {
			mismatch.appendText(" was ")
					.appendValue(numMatches);
			mismatch.appendText(" because: ");
		}

		mismatch.appendText(mismatches.toString());
	}


	/**
	 * Describe the condition of the concrete matcher.
	 * @param description {@link Description} to append to.
	 * @return changed {@link Description}.
	 */
	protected abstract Description describeCondition(Description description);

	/**
	 * Prefix for the matcher description.
	 * @return concrete prefix.
	 */
	protected abstract String prefix();

	/**
	 * Condition for while loop
	 * @param numMatches number of positive matches.
	 * @param numMismatches number negative matches.
	 * @return true while condition is valid.
	 */
	protected abstract boolean validWhile(int numMatches, int numMismatches);

	/**
	 * Condition to check after the loop is finished.
	 * @param numMatches number of positive matches
	 * @return true if condition is met at the finish.
	 */
	protected boolean validAfter(int numMatches) {
		return true;
	}

}

