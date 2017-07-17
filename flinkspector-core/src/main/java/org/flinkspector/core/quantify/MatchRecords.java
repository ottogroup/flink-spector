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

import com.google.common.annotations.VisibleForTesting;
import org.flinkspector.core.quantify.assertions.MatcherCombiner;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

/**
 * Lets you build a list of assertion and define how often they should match your output
 * <pre>
 * {@code
 * new MatchRecords<Integer>()
 * 		.assertThat(greaterThan(21))
 * 		.assertThat(lessThan(11))
 * 		anyOfThem().onEachRecord();
 * }
 * </pre>
 *
 * @param <T>
 */
public class MatchRecords<T> {

    /**
     * List of {@link Matcher} representing the assertions.
     */
    private List<Matcher<? super T>> matchers = new ArrayList<>();

    /**
     * Expect the record to fulfill at least one of the specified asserts.
     *
     * @return {@link RecordsMatcher} to define the number of matching records in
     * the output.
     */
    public RecordsMatcher<T> anyOfThem() {
        return new RecordsMatcher<>(MatcherCombiner.any(matchers));
    }

    /**
     * Expect the record to fulfill exactly one of the specified asserts.
     *
     * @return {@link RecordsMatcher} to define the number of matching records in
     * the output.
     */
    public RecordsMatcher<T> oneOfThem() {
        return new RecordsMatcher<>(MatcherCombiner.one(matchers));
    }


    /**
     * Add a {@link Matcher} to the list of assertions to verify.
     *
     * @param matcher testing the output records
     */
    public MatchRecords<T> assertThat(Matcher<? super T> matcher) {
        matchers.add(matcher);
        return this;
    }
//	/**
//	 * Expect the record to fulfill all of the specified asserts.
//	 *
//	 * @return {@link RecordsMatcher} to define the number of matching records in
//	 * the output.
//	 */
//	public RecordsMatcher<T> eachOfThem() {
//		return new RecordsMatcher<>(TupleMapMatchers.each(matchers, mask));
//	}

    /**
     * Expect the record to fulfill at least exactly n of the specified asserts.
     *
     * @param n number of expected positive matches.
     * @return {@link RecordsMatcher} to define the number of matching records in
     * the output.
     */
    public RecordsMatcher<T> exactlyNOfThem(int n) {
        return new RecordsMatcher<>(MatcherCombiner.exactly(matchers, n));
    }

    /**
     * Expect the record to fulfill at least a number of the specified asserts.
     *
     * @param n number of minimum matches.
     * @return {@link RecordsMatcher} to define the number of matching records in
     * the output.
     */
    public RecordsMatcher<T> atLeastNOfThem(int n) {
        return new RecordsMatcher<>(MatcherCombiner.atLeast(matchers, n));
    }

    /**
     * Expect record to fulfill at most a number of the specified asserts.
     *
     * @param n number of maximum matches.
     * @return {@link RecordsMatcher} to define the number of matching records in
     * the output.
     */
    public RecordsMatcher<T> atMostNOfThem(int n) {
        return new RecordsMatcher<>(MatcherCombiner.atMost(matchers, n));
    }

    /**
     * Creates a {@link OutputMatcher} matching when at least one
     * inspected tuple returns a positive match.
     *
     * @return {@link OutputMatcher}
     */
    public OutputMatcher<T> onAnyRecord() {
        RecordsMatcher<T> matcher = new RecordsMatcher<>(MatcherCombiner.each(matchers));
        return OutputMatcherFactory.create(matcher.onAnyRecord());
    }

    /**
     * Creates a {@link OutputMatcher} matching when all
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcher}
     */
    public OutputMatcher<T> onEachRecord() {
        RecordsMatcher<T> matcher = new RecordsMatcher<>(MatcherCombiner.each(matchers));
        return OutputMatcherFactory.create(matcher.onEachRecord());
    }

    /**
     * Creates a {@link OutputMatcher} matching when exactly one inspected tuple
     * return a positive match.
     *
     * @return {@link OutputMatcher}
     */
    public OutputMatcher<T> onOneRecord() {
        RecordsMatcher<T> matcher = new RecordsMatcher<>(MatcherCombiner.each(matchers));
        return OutputMatcherFactory.create(matcher.onOneRecord());
    }

    /**
     * Creates a {@link OutputMatcher} matching when no inspected tuple
     * return a positive match.
     *
     * @return {@link OutputMatcher}
     */
    public OutputMatcher<T> onNoRecord() {
        RecordsMatcher<T> matcher = new RecordsMatcher<>(MatcherCombiner.each(matchers));
        return OutputMatcherFactory.create(matcher.onNoRecord());
    }

    /**
     * Creates a {@link OutputMatcher} matching when a exact number of
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcher}
     */
    public OutputMatcher<T> onExactlyNRecords(int n) {
        RecordsMatcher<T> matcher = new RecordsMatcher<>(MatcherCombiner.each(matchers));
        return OutputMatcherFactory.create(matcher.onExactlyNRecords(n));
    }

    /**
     * Creates a {@link OutputMatcher} matching when at least a number of
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcher}
     */
    public OutputMatcher<T> onAtLeastNRecords(int n) {
        RecordsMatcher<T> matcher = new RecordsMatcher<>(MatcherCombiner.each(matchers));
        return OutputMatcherFactory.create(matcher.onAtLeastNRecords(n));
    }

    /**
     * Creates a {@link OutputMatcher} matching when at most a number of
     * inspected tuples return a positive match.
     *
     * @return {@link OutputMatcher}
     */
    public OutputMatcher<T> onAtMostNRecords(int n) {
        RecordsMatcher<T> matcher = new RecordsMatcher<>(MatcherCombiner.each(matchers));
        return OutputMatcherFactory.create(matcher.onAtMostNRecords(n));
    }

    @VisibleForTesting
    List<Matcher<? super T>> getMatchers() {
        return matchers;
    }


}
