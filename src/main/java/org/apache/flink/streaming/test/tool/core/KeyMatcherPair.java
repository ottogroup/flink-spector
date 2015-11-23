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

import org.hamcrest.Matcher;

/**
 * Data structure mapping a {@link String} key to a {@link Matcher}.
 * <p>
 * Used for defining matchers that work on {@link TupleMap}.
 */
public class KeyMatcherPair {

	public final String key;

	public final Matcher matcher;

	public KeyMatcherPair(String key, Matcher matcher) {
		this.matcher = matcher;
		this.key = key;
	}

	/**
	 * Factory method.
	 * @param key string key.
	 * @param matcher {@link Matcher}.
	 * @return new instance of this class.
	 */
	public static KeyMatcherPair of(
			String key,
			Matcher matcher
	) {
		return new KeyMatcherPair(key, matcher);
	}

}
