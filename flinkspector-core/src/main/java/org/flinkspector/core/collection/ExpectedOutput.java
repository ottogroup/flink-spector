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

import org.flinkspector.core.table.OutputMatcher;
import org.hamcrest.Description;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class is used to define expectations
 * for the output of a endpoint.
 * @param <T>
 */
public class ExpectedOutput<T> extends OutputMatcher<T> {

	/** collection of expected records */
	private List<T> expectedRecords;
	/** matcher to test the output */
	private MatcherBuilder<T> matcher;

	public ExpectedOutput() {
		expectedRecords = new ArrayList<>();
	}

	public static <T> ExpectedOutput<T> create(T record) {
		if(record == null) {
			throw new IllegalArgumentException("Record has too be not null!");
		}
		ExpectedOutput<T> output = new ExpectedOutput<>();
		return output.expect(record);
	}

	public static <T> ExpectedOutput<T> create(Collection<T> records) {
		if(records == null) {
			throw new IllegalArgumentException("Record has too be not null!");
		}
		ExpectedOutput<T> output = new ExpectedOutput<>();
		return output.expectAll(records);
	}

	/**
	 * Provides the {@link MatcherBuilder} to define
	 * expectations for the test
	 * @return builder for refining expectations
	 */
	public MatcherBuilder<T> refine() {
		if (matcher == null) {
			matcher = new MatcherBuilder<>(expectedRecords);
		}
		return matcher;
	}

	/**
	 * Adds an record to the collection of expected output
	 * @param record to add
	 */
	public ExpectedOutput<T> expect(T record) {
		if(record == null) {
			throw new IllegalArgumentException("Record has too be not null!");
		}
		expectedRecords.add(record);
		return this;
	}

	/**
	 * Adds an record to the collection of expected output
	 * @param record to add
	 */
	public ExpectedOutput<T> expect(T record, int times) {
		if(record == null) {
			throw new IllegalArgumentException("Record has too be not null!");
		}
		expectedRecords.add(record);
		return this;
	}

	/**
	 * Adds a {@link Collection} of records to the expected output
	 * @param records to add
	 */
	public ExpectedOutput<T> expectAll(Collection<T> records) {
		if(records == null) {
			throw new IllegalArgumentException("Record has too be not null!");
		}
		expectedRecords.addAll(records);
		return this;
	}

	/**
	 * Expect the current output a number of times
	 *
	 * @param times number of times the input ist will be repeated
	 */
	public ExpectedOutput<T> repeatAll(int times) {
		List<T> toAppend = new ArrayList<>();
		for (int i = 0; i < times; i++) {
			toAppend.addAll(expectedRecords);
		}
		expectedRecords.addAll(toAppend);
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
