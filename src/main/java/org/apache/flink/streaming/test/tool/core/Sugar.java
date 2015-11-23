/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.core;

import org.apache.flink.streaming.test.tool.core.input.After;
import org.apache.flink.streaming.test.tool.core.input.Before;
import org.apache.flink.streaming.test.tool.core.input.EventTimeInputBuilder;

import java.util.concurrent.TimeUnit;

public class Sugar {

	public static After after(long span,TimeUnit unit) {
		return After.period(span,unit);
	}

	public static Before before(long span,TimeUnit unit) {
		return Before.period(span, unit);
	}

	public static <T> EventTimeInputBuilder<T> startWith(T elem) {
		return EventTimeInputBuilder.create(elem);
	}

	public static int times(int n) {
		return n;
	}

	public static final TimeUnit seconds = TimeUnit.SECONDS;
	public static final TimeUnit minutes = TimeUnit.MINUTES;
	public static final TimeUnit hours = TimeUnit.HOURS;
	public static final String ignore = null;
}
