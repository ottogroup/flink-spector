package org.apache.flink.streaming.test.tool.runtime;

/**
 * The default trigger used by the {@link StreamTestEnvironment}.
 * This trigger will not terminate the listener prematurely.
 */
public class DefaultTestTrigger implements VerifyFinishedTrigger {

	@Override
	public boolean onRecord(Object record) {
		return false;
	}

	@Override
	public boolean onRecordCount(long count) {
		return false;
	}
}
