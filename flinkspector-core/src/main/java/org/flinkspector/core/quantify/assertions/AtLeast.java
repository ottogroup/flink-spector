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

import org.apache.flink.api.java.tuple.Tuple;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at least a number of the specified matchers.
 */
public class AtLeast<T> extends UntilCombineMatcher<T> {

    private final int n;

    /**
     * Default constructor
     *
     * @param matchers {@link Iterable} of {@link Matcher}
     * @param n        number of expected matches
     */
    public AtLeast(Iterable<Matcher<? super T>> matchers, int n) {
        super(matchers);
        this.n = n;
    }

    @Factory
    public static <T> AtLeast<T> atLeast(Iterable<Matcher<? super T>> matchers, int n) {
        return new AtLeast<>(matchers, n);
    }

    public Description describeCondition(Description description) {
        return description.appendText("at least ").appendValue(n);
    }

    @Override
    public boolean validWhen(int matches, int possibleMatches) {
        return matches == n;
    }

    @Override
    public String prefix() {
        return "at least ";
    }
}
