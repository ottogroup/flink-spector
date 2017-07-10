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

package org.flinkspector.core.runtime;


/**
 * Implement this interface to verify the input
 * from test endpoints.
 *
 * @param <T>
 */
public interface OutputVerifier<T> {

	/**
	 * This method is called when the {@link OutputHandler} has
	 * been successfully initialized
	 */
	void init();

	/**
	 * This method is called when a record has arrived
	 * at the test sink.
	 * <p>
	 * The method must throw a {@link FlinkTestFailedException},
	 * which wraps the actual exception, if the test has failed.
	 *
	 * @param record that is received by the test sink
	 * @throws Exception
	 */
	void receive(T record) throws Exception;

	/**
	 * This method is called by the {@link OutputHandler}
	 * when the test is finished and the last record has
	 * been received.
	 * <p>
	 * The method must throw a {@link FlinkTestFailedException},
	 * which wraps the actual exception, if the test has failed.
	 *
	 * @throws Exception
	 */
	void finish() throws Exception;

}
