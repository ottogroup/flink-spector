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

package org.flinkspector.core.quantify;

import org.flinkspector.core.runtime.FlinkTestFailedException;
import org.flinkspector.core.runtime.SimpleOutputVerifier;
import org.hamcrest.Matcher;
import org.junit.Assert;

import java.util.List;

public class HamcrestVerifier<T> extends SimpleOutputVerifier<T> {

	private final Matcher<? super Iterable<T>> matcher;

	public HamcrestVerifier(Matcher<? super Iterable<T>> matcher) {
		this.matcher = matcher;
	}

	@Override
	public void verify(List<T> output) throws FlinkTestFailedException {
		Assert.assertThat(output, matcher);
	}

	public static <T> HamcrestVerifier<T> create(Matcher<? super Iterable<T>> matcher) {
		return new HamcrestVerifier<T>(matcher);
	}
}
