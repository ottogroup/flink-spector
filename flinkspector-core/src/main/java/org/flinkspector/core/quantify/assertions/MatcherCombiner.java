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
import org.hamcrest.Matcher;

/**
 * Offers a set of factory methods to startWith {{@link Matcher}s,
 * taking an {@link Iterable} of {@link Matcher}s
 * and ensure a certain number of positive matches.
 */
public class MatcherCombiner {

    /**
     * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
     * fulfill at least one of the specified matchers.
     *
     * @param matchers {@link Iterable} of matchers
     * @param <T>
     * @return new {@link Matcher}
     */
    public static <T> Matcher<T> any(Iterable<Matcher<? super T>> matchers) {
        return Any.any(matchers);
    }

    /**
     * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
     * fulfill all of the specified matchers.
     *
     * @param matchers {@link Iterable} of matchers
     * @param <T>
     * @return new {@link Matcher}
     */
    public static <T> Matcher<T> each(Iterable<Matcher<? super T>> matchers) {
        return Each.each(matchers);
    }

    /**
     * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
     * fulfill exactly one of the specified matchers.
     *
     * @param matchers {@link Iterable} of matchers
     * @param <T>
     * @return new {@link Matcher}
     */
    public static <T> Matcher<T> one(Iterable<Matcher<? super T>> matchers) {
        return One.one(matchers);
    }

    /**
     * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
     * fulfill an exact number of the specified matchers.
     *
     * @param matchers {@link Iterable} of matchers
     * @param n        number of matches
     * @param <T>
     * @return new {@link Matcher}
     */
    public static <T> Matcher<T> exactly(Iterable<Matcher<? super T>> matchers,
                                         int n) {
        return Exactly.exactly(matchers, n);
    }

    /**
     * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
     * fulfill at least a number of the specified matchers.
     *
     * @param matchers {@link Iterable} of matchers
     * @param n        number of matches
     * @param <T>
     * @return new {@link Matcher}
     */
    public static <T> Matcher<T> atLeast(Iterable<Matcher<? super T>> matchers,
                                         int n) {
        return AtLeast.atLeast(matchers, n);
    }

    /**
     * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
     * fulfill at most a number of the specified matchers.
     *
     * @param matchers {@link Iterable} of matchers
     * @param n        number of matches
     * @param <T>
     * @return new {@link Matcher}
     */
    public static <T> Matcher<T> atMost(Iterable<Matcher<? super T>> matchers,
                                        int n) {
        return AtMost.atMost(matchers, n);
    }

}
