package org.apache.flink.streaming.test.tool.core;

import org.apache.flink.streaming.test.tool.runtime.VerifyFinishedTrigger;
import org.hamcrest.Matcher;

/**
 * This trigger uses a {@link Matcher} to determine whether to finish a validation,
 * on receiving a record.
 * <p>
 * If the provided {@link Matcher} returns true the test will be finished early.
 * @param <OUT>
 */
public class MatcherTrigger<OUT> implements VerifyFinishedTrigger<OUT> {

	/**
	 * {@link Matcher} used internally.
	 */
	private final Matcher<OUT> matcher;

	/**
	 * Default constructor
	 * @param matcher used for triggering.
	 */
	public MatcherTrigger(Matcher<OUT> matcher) {
		this.matcher = matcher;
	}

	@Override
	public boolean onRecord(OUT record) {
		return matcher.matches(record);
	}

	@Override
	public boolean onRecordCount(long count) {
		return false;
	}

	public static <T> MatcherTrigger on(Matcher<T> matcher) {
		return new MatcherTrigger<T>(matcher);
	}
}
