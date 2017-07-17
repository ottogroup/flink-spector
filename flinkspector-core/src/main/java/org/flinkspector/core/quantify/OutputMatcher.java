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

import org.hamcrest.TypeSafeMatcher;

/**
 * Used to cover up {@link TypeSafeMatcher}s for {@link Iterable}s to provide a more consize
 * matcher type to use for verifying test results.
 *
 * @param <T>
 */
public abstract class OutputMatcher<T> extends TypeSafeMatcher<Iterable<T>> {

    @Override
    protected abstract boolean matchesSafely(Iterable<T> item);
}
