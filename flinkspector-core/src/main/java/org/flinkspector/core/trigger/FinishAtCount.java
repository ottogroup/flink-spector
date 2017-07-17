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

package org.flinkspector.core.trigger;

/**
 * Provides {@link VerifyFinishedTrigger} that triggers
 * when a defined count of output records has been reached.
 */
public class FinishAtCount implements VerifyFinishedTrigger {

    final long maxCount;

    /**
     * Default constructor.
     *
     * @param maxCount of records to trigger on.
     */
    public FinishAtCount(long maxCount) {
        this.maxCount = maxCount;
    }

    /**
     * Factory method.
     *
     * @param maxCount of records to trigger on.
     * @return new instance of {@link FinishAtCount}.
     */
    public static FinishAtCount of(long maxCount) {
        return new FinishAtCount(maxCount);
    }

    @Override
    public boolean onRecord(Object record) {
        return false;
    }

    @Override
    public boolean onRecordCount(long count) {
        return count >= maxCount;
    }
}
