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

package io.flinkspector.core.quantify.records;

import org.hamcrest.Description;
import org.hamcrest.Matcher;


/**
 * Provides a {@link Matcher} that is successful if each of
 * the items in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class OnEach<T> extends WhileList<T> {

    /**
     * Default Constructor
     *
     * @param matcher to apply to {@link Iterable}.
     */
    public OnEach(Matcher<T> matcher) {
        super(matcher);
    }

    public static <T> OnEach<T> each(Matcher<T> matcher) {
        return new OnEach<>(matcher);
    }

    @Override
    protected Description describeCondition(Description description) {
        return description.appendText("<all>");
    }

    @Override
    public boolean validWhile(int numMatches, int numMismatches) {
        return numMismatches <= 0;
    }

    @Override
    public String prefix() {
        return "each record";
    }
}
