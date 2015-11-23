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

package org.apache.flink.streaming.test.tool.core.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if exactly one
 * item in the examined {@link Iterable} is a positive match.
 * @param <T>
 */
public class OneOf<T> extends WhileRecord<T> {

	/**
	 * Default constructor
	 * @param matcher to apply to the {@link Iterable}
	 */
	public OneOf(Matcher<T> matcher) {
		super(matcher);
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("exactly ").appendValue(1);
	}

	@Override
	public String prefix() {
		return "one of ";
	}

	@Override
	public boolean validWhile(int matches, int mismatches) {
		return matches <= 1;
	}

	@Override
	public boolean validAfter(int matches) {
		return matches == 1;
	}

	@Factory
	public static <T> OneOf<T> one(Matcher<T> matcher) {
		return new OneOf<T>(matcher);
	}
}
