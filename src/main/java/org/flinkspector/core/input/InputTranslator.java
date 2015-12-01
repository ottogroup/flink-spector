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

package org.flinkspector.core.input;

import java.util.ArrayList;
import java.util.List;

/**
 * A InputTranslator transforms an {@link Input} object into an
 * {@link Input} of another type.
 * <p>
 * Implement this interface to translate concise {@link Input} to the type of input
 * required by your test.
 * E.g: translate from tuples into json strings.
 *
 * @param <IN>
 * @param <OUT>
 */
public abstract class InputTranslator<IN,OUT> implements Input<OUT> {

	private Input<IN> input;

	protected InputTranslator(Input<IN> input) {
		this.input = input;
	}

	abstract protected OUT translate(IN elem);

	@Override
	public List<OUT> getInput() {
		List<OUT> out = new ArrayList<OUT>();
		for (IN elem: input.getInput()) {
			out.add(translate(elem));
		}
		return out;
	}
}
