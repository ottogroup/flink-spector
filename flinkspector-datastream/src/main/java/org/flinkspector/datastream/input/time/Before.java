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

package org.flinkspector.datastream.input.time;

import java.util.concurrent.TimeUnit;

/**
 * Helper for defining a time span between to StreamRecords
 */
public class Before extends TimeSpan {

	private long timeSpan;

	public static Before period(long time, TimeUnit timeUnit) {
		return new Before(time, timeUnit);
	}

	private Before(long time, TimeUnit timeUnit) {
		this.timeSpan = timeUnit.toMillis(time);
	}

	/**
	 * Getter for defined time span
	 *
	 * @return time span in milliseconds
	 */
	@Override
	public long getTimeSpan() {
		return timeSpan * -1;
	}
}
