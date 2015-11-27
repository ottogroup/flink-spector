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

package org.apache.flink.core.trigger;
import org.apache.flink.core.runtime.OutputListener;

/**
 * This trigger can be used to stop a
 * {@link OutputListener}
 * prematurely, and finalizing the {@link org.apache.flink.core.runtime.OutputVerifier}
 * registered for this listener.
 */
public interface VerifyFinishedTrigger<OUT> {

	/**
	 * Returns true if the listener should be
	 * closed on receiving a record.
	 * @param record current record received by the listener.
	 * @return true if close.
	 */
	boolean onRecord(OUT record);

	/**
	 * Determines if the listener should be
	 * closed on receiving a count of records.
	 * @param count current count of records received by the listener.
	 * @return true if close.
	 */
	boolean onRecordCount(long count);
}
