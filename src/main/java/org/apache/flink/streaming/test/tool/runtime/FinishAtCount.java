package org.apache.flink.streaming.test.tool.runtime;

/**
 * Provides {@link VerifyFinishedTrigger} that triggers
 * when a defined count of output records has been reached.
 */
public class FinishAtCount implements VerifyFinishedTrigger {

	final long maxCount;

	/**
	 * Default constructor.
	 *
	 * @param maxCount of records to trigger on.
	 */
	public FinishAtCount(long maxCount) {
		this.maxCount = maxCount;
	}

	@Override
	public boolean onRecord(Object record) {
		return false;
	}

	@Override
	public boolean onRecordCount(long count) {
		return count >= maxCount;
	}

	/**
	 * Factory method.
	 * @param maxCount of records to trigger on.
	 * @return new instance of {@link FinishAtCount}.
	 */
	public static FinishAtCount of(long maxCount) {
		return new FinishAtCount(maxCount);
	}
}
