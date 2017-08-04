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

package io.flinkspector.core.runtime;

/**
 * Throw this exception if your {@link OutputVerifier}
 * fails a not valid
 * Used as an wrapper around the specific exception thrown by the used Test Framework.
 */
public class FlinkTestFailedException extends Exception {
    public FlinkTestFailedException(Throwable throwable) {
        super(throwable);
        if (throwable == null) {
            throw new IllegalArgumentException("The cause has to be defined. " +
                    "It will be unwrapped by the runtime later.");
        }

    }
}
