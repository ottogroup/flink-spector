package org.apache.flink.streaming.test.tool.runtime;

/**
 * Throw this exception if your {@link org.apache.flink.streaming.test.tool.runtime.output.OutputVerifier}
 * fails a not valid
 * Used as an wrapper around the specific exception thrown by the used Test Framework.
 */
public class StreamTestFailedException extends Exception {
	public StreamTestFailedException(String msg, Throwable throwable) {
		super(msg,throwable);
		if(throwable == null) {
			throw new IllegalArgumentException("The cause has to be defined. " +
					"It will be unwrapped by the runtime later.");
		}

	}
}
