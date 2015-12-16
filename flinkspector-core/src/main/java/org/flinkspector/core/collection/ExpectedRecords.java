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

package org.flinkspector.core.collection;

import org.flinkspector.core.quantify.OutputMatcher;
import org.hamcrest.Description;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class is used to define expectations
 * for the output of a endpoint.
 * @param <T>
 */
public class ExpectedRecords<T> extends OutputMatcher<T> {

	/** list of expected records */
	private List<T> expectedOutput;
	/** matcher to test the output */
	private MatcherBuilder<T> matcher;

	public ExpectedRecords() {
		expectedOutput = new ArrayList<>();
	}

	public static <T> ExpectedRecords<T> create(T record) {
		ExpectedRecords<T> output = new ExpectedRecords<>();
		return output.expect(record);
	}

	public static <T> ExpectedRecords<T> create(Collection<T> records) {
		ExpectedRecords<T> output = new ExpectedRecords<>();
		return output.expectAll(records);
	}

	/**
	 * Provides the {@link MatcherBuilder} to define
	 * expectations for the test
	 * @return builder for refining expectations
	 */
	public MatcherBuilder<T> refine() {
		if (matcher == null) {
			matcher = new MatcherBuilder<>(expectedOutput);
		}
		return matcher;
	}

	/**
	 * Adds an record to the list of expected output
	 * @param record to add
	 */
	public ExpectedRecords<T> expect(T record) {
		expectedOutput.add(record);
		return this;
	}

	/**
	 * Adds a {@link Collection} of records to the expected output
	 * @param records to add
	 */
	public ExpectedRecords<T> expectAll(Collection<T> records) {
		expectedOutput.addAll(records);
		return this;
	}

	@Override
	public boolean matchesSafely(Iterable<T> output) {
		return refine().matches(output);
	}

	@Override
	public void describeTo(Description description) {
		refine().describeTo(description);
	}

//	public <T> getValidator() {
//		return matcher;
//	}

}
