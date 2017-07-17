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

package org.flinkspector.core.quantify.records;

import com.google.common.collect.Iterables;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public abstract class UntilList<T> extends TypeSafeDiagnosingMatcher<Iterable<T>> {

    private final Matcher<T> matcher;

    public UntilList(Matcher<T> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(Iterable<T> objects, Description mismatch) {
        int numMatches = 0;
        int numMismatches = 0;
        int possibleMatches = Iterables.size(objects);
        int i = 0;
        Description mismatches = new StringDescription();
        for (T item : objects) {

            if (!matcher.matches(item)) {
                if (numMismatches < 10) {
                    matcher.describeMismatch(item, mismatches);
                    mismatches.appendText(" on record #" + (i + 1));
                }
                numMismatches++;
            } else {
                numMatches++;

                if (validWhen(numMatches, possibleMatches)) {
                    return true;
                }
            }
            i++;
        }
        describeMismatch(numMatches, mismatch, mismatches);
        return false;
    }

    private void describeMismatch(int matches,
                                  Description mismatch,
                                  Description mismatches) {
        mismatch.appendText("expected matches to be ");
        describeCondition(mismatch);
        mismatch.appendText(", was ")
                .appendValue(matches)
                .appendText(" because:")
                .appendText(mismatches.toString());
    }

    protected abstract Description describeCondition(Description description);

    protected abstract boolean validWhen(int matches, int possibleMatches);

    protected abstract String prefix();

    @Override
    public void describeTo(Description description) {
        description.appendText(prefix()).appendText(", to be ").appendDescriptionOf(matcher);
    }


}


