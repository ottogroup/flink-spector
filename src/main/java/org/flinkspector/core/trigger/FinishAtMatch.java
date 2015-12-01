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

import org.hamcrest.Matcher;

/**
 * This trigger uses a {@link Matcher} to determine whether to finish a validation,
 * on receiving a record.
 * <p>
 * If the provided {@link Matcher} returns true the test will be finished early.
 * @param <OUT>
 */
public class FinishAtMatch<OUT> implements VerifyFinishedTrigger<OUT> {

	/**
	 * {@link Matcher} used internally.
	 */
	private final Matcher<OUT> matcher;

	/**
	 * Default constructor
	 * @param matcher used for triggering.
	 */
	public FinishAtMatch(Matcher<OUT> matcher) {
		this.matcher = matcher;
	}

	@Override
	public boolean onRecord(OUT record) {
		return matcher.matches(record);
	}

	@Override
	public boolean onRecordCount(long count) {
		return false;
	}

	public static <T> FinishAtMatch<T> of(Matcher<T> matcher) {
		return new FinishAtMatch<T>(matcher);
	}
}
