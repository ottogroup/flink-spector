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

package io.flinkspector.datastream.input.time;

/**
 * Specifies a moment for a record to generate a timestamp.
 */
public abstract class Moment {

    /**
     * Getter for defined time span
     *
     * @param currentTimestamp the timestamp of the current record
     * @return time span in milliseconds
     */
    public abstract long getTimestamp(long currentTimestamp);

    public long getShift() {
        return 0;
    }
}
