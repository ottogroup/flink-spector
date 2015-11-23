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

package org.apache.flink.streaming.test.tool.core.assertion;

import org.apache.flink.streaming.test.tool.runtime.output.SimpleOutputVerifier;
import org.apache.flink.streaming.test.tool.runtime.StreamTestFailedException;
import org.hamcrest.Matcher;
import org.junit.Assert;

import java.util.List;

public class HamcrestVerifier<T> extends SimpleOutputVerifier<T> {

	public final Matcher<Iterable<T>> matcher;

	public HamcrestVerifier(Matcher<Iterable<T>> matcher) {
		this.matcher = matcher;
	}

	@Override
	public void verify(List<T> output) throws StreamTestFailedException {
		try {
			Assert.assertThat(output, matcher);
		} catch (AssertionError e) {
			throw new StreamTestFailedException("Assertion Failed!",e);
		}
	}
}
