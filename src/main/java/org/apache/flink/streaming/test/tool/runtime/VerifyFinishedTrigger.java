package org.apache.flink.streaming.test.tool.runtime;


/**
 * This trigger can be used to stop a
 * {@link org.apache.flink.streaming.test.tool.runtime.messaging.OutputListener}
 * prematurely, and finalizing the {@link org.apache.flink.streaming.test.tool.runtime.output.OutputVerifier}
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
