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
 * Sets the timestamp of a record to a value which fits into a certain window.
 */
public class InWindow implements Moment {

	private final long timeSpan;

	public static InWindow to(long time, TimeUnit timeUnit){
		return new InWindow(time,timeUnit);
	}

	private InWindow(long time,TimeUnit timeUnit) {
		this.timeSpan = timeUnit.toMillis(time);
	}

	@Override
	public long getTimestamp(long currentTimestamp) {
		return timeSpan - 1;
	}
}
