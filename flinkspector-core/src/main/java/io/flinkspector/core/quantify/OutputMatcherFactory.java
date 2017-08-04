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

package io.flinkspector.core.quantify;

import org.hamcrest.Description;
import org.hamcrest.Matcher;

/**
 * Factory for {@link OutputMatcher}
 *
 * @param <T>
 */
public class OutputMatcherFactory<T> {

    /**
     * Wraps a {@link Matcher} working with {@link Iterable} to provide an
     * {@link OutputMatcher}
     *
     * @param matcher to wrap
     * @param <T>
     * @return {@link OutputMatcher}
     */
    public static <T> OutputMatcher<T> create(final Matcher<? super Iterable<T>> matcher) {
        return new OutputMatcher<T>() {

            @Override
            protected boolean matchesSafely(Iterable<T> item) {
                return matcher.matches(item);
            }

            @Override
            public void describeMismatchSafely(Iterable<T> output,
                                               Description mismatchDescription) {
                matcher.describeMismatch(output, mismatchDescription);
            }

            @Override
            public void describeTo(Description description) {
                matcher.describeTo(description);
            }
        };
    }
}
