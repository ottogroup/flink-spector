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

package org.apache.flink.core;

import org.apache.flink.core.set.ExpectedOutput;
import org.apache.flink.core.set.MatcherBuilder;
import org.apache.flink.streaming.test.input.EventTimeInputBuilder;
import org.apache.flink.core.input.InputBuilder;
import org.apache.flink.streaming.test.input.time.After;
import org.apache.flink.streaming.test.input.time.Before;

import java.util.concurrent.TimeUnit;

public class Sugar {

	/**
	 * Creates an {@link After} object.
	 * @param span length of span
	 * @param unit of time
	 * @return {@link After}
	 */
	public static After after(long span,TimeUnit unit) {
		return After.period(span,unit);
	}

	/**
	 * Creates an {@link Before} object.
	 * @param span length of span
	 * @param unit of time
	 * @return {@link Before}
	 */
	public static Before before(long span,TimeUnit unit) {
		return Before.period(span, unit);
	}

	public static <T> EventTimeInputBuilder<T> startWith(T record) {
		return EventTimeInputBuilder.create(record);
	}

	public static <T> InputBuilder<T> emit(T elem) {
		return InputBuilder.create(elem);
	}

	public static <T> ExpectedOutput<T> expectOutput(T record) {
		return ExpectedOutput.create(record);
	}

	public static int times(int n) {
		return n;
	}


	public static final MatcherBuilder.Order strict = MatcherBuilder.Order.STRICT;
	public static final MatcherBuilder.Order notStrict = MatcherBuilder.Order.NONSTRICT;
	public static final TimeUnit seconds = TimeUnit.SECONDS;
	public static final TimeUnit minutes = TimeUnit.MINUTES;
	public static final TimeUnit hours = TimeUnit.HOURS;
	public static final String ignore = null;
}
