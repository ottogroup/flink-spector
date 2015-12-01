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

import org.flinkspector.core.KeyMatcherPair;
import org.flinkspector.core.table.TupleMap;
import org.flinkspector.core.table.TupleMask;
import org.apache.flink.api.java.tuple.Tuple;
import org.flinkspector.core.KeyMatcherPair;
import org.flinkspector.core.table.TupleMap;
import org.flinkspector.core.table.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

abstract public class WhileTuple<T extends Tuple> extends TypeSafeDiagnosingMatcher<T> {
	//TODO play with description

	private final Iterable<KeyMatcherPair> keyMatcherPairs;
	private final TupleMask<T> table;

	public WhileTuple(Iterable<KeyMatcherPair> matchers,
					TupleMask<T> table) {
		this.keyMatcherPairs = matchers;
		this.table = table;
	}

	@Override
	public boolean matchesSafely(T tuple, Description mismatch) {

		TupleMap tupleMap = table.apply(tuple);
		int matches = 0;
		Description mismatches = new StringDescription();
		mismatch.appendText("\n          ");
		for (KeyMatcherPair keyMatcherPair : keyMatcherPairs) {
			String key = keyMatcherPair.key;
			Object object = tupleMap.get(key);
			Matcher matcher = keyMatcherPair.matcher;
			if (!matcher.matches(object)) {
				mismatches
						.appendText("[" + key + "] ")
						.appendDescriptionOf(matcher)
						.appendText(", ");
				matcher.describeMismatch(object, mismatches);
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
		List<Matcher> matchers = new ArrayList<>();
		description.appendText(prefix());
		description.appendText("(");
		for (KeyMatcherPair m : keyMatcherPairs) {
			if(!description.toString().endsWith("(")) {
				description.appendText("; ");
			}
			description.appendText("[" + m.key + "] ");
			description.appendDescriptionOf(m.matcher);
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
