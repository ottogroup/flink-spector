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

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if at most n
 * items in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class OnAtMost<T> extends WhileList<T> {

    private final int n;

    /**
     * Default constructor
     *
     * @param matcher to apply to the {@link Iterable}
     * @param n       number of expected matches
     */
    public OnAtMost(Matcher<T> matcher, int n) {
        super(matcher);
        this.n = n;
    }

    @Factory
    public static <T> OnAtMost<T> atMost(Matcher<T> matcher, int n) {
        return new OnAtMost<T>(matcher, n);
    }

    @Override
    protected Description describeCondition(Description description) {
        return description.appendText("at most ").appendValue(n);
    }

    @Override
    public String prefix() {
        return "at most " + n + " records";
    }

    @Override
    public boolean validWhile(int numMatches, int numMismatches) {
        return numMatches <= n;
    }
}