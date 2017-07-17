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

package org.flinkspector.core.quantify;

import org.flinkspector.core.quantify.records.ListQuantifyMatchers;
import org.hamcrest.Matcher;

/**
 * Takes a {@link Matcher} and converts it into a {@link OutputMatcher}
 * matching an iterable of tuples.
 * Provides a set of methods to define the number of matches necessary to
 * fulfill the expectation.
 *
 * @param <T>
 */
public class RecordsMatcher<T> {

    /**
     * wrapped {@link Matcher}
     */
    private final Matcher<T> matcher;

    /**
     * Default constructor.
     *
     * @param matcher
     */
    public RecordsMatcher(Matcher<T> matcher) {
        this.matcher = matcher;
    }

    /**
     * Creates a {@link OutputMatcherFactory} matching when at least one
     * inspected tuple returns a positive match.
     *
     * @return {@link OutputMatcherFactory}
     */
    public OutputMatcher<T> onAnyRecord() {
        return OutputMatcherFactory.create(ListQuantifyMatchers.any(matcher));
    }

    /**
     * Creates a {@link OutputMatcherFactory} matching when all
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcherFactory}
     */
    public OutputMatcher<T> onEachRecord() {
        return OutputMatcherFactory.create(ListQuantifyMatchers.each(matcher));
    }

    /**
     * Creates a {@link OutputMatcherFactory} matching when exactly one inspected tuple
     * return a positive match.
     *
     * @return {@link OutputMatcherFactory}
     */
    public OutputMatcher<T> onOneRecord() {
        return OutputMatcherFactory.create(ListQuantifyMatchers.one(matcher));
    }

    /**
     * Creates a {@link OutputMatcherFactory} matching when no inspected tuple
     * return a positive match.
     *
     * @return {@link OutputMatcherFactory}
     */
    public OutputMatcher<T> onNoRecord() {
        return OutputMatcherFactory.create(ListQuantifyMatchers.none(matcher));
    }

    /**
     * Creates a {@link OutputMatcherFactory} matching when a exact number of
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcherFactory}
     */
    public OutputMatcher<T> onExactlyNRecords(int n) {
        return OutputMatcherFactory.create(ListQuantifyMatchers.exactly(matcher, n));
    }

    /**
     * Creates a {@link OutputMatcherFactory} matching when at least a number of
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcherFactory}
     */
    public OutputMatcher<T> onAtLeastNRecords(int n) {
        return OutputMatcherFactory.create(ListQuantifyMatchers.atLeast(matcher, n));
    }

    /**
     * Creates a {@link OutputMatcherFactory} matching when at most a number of
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcherFactory}
     */
    public OutputMatcher<T> onAtMostNRecords(int n) {
        return OutputMatcherFactory.create(ListQuantifyMatchers.atMost(matcher, n));
    }


}