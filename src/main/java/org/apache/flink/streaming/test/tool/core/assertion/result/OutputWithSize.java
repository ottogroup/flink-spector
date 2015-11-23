package org.apache.flink.streaming.test.tool.core.assertion.result;

import com.google.common.collect.Iterables;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.core.IsEqual;

/**
 * Provides a matcher for output that matches when the <code>size</code> of the output
 * satisfies the specified matcher.
 * @param <T>
 */
public class OutputWithSize<T> extends TypeSafeDiagnosingMatcher<Iterable<T>> {

	Matcher<? super Integer> sizeMatcher;

	public OutputWithSize(Matcher<? super Integer> sizeMatcher) {
		this.sizeMatcher = sizeMatcher;
	}

	@Override
	protected boolean matchesSafely(Iterable<T> item, Description mismatchDescription) {
		int size = Iterables.size(item);
		boolean matches = sizeMatcher.matches(size);
		if(!matches) {
			sizeMatcher.describeMismatch(size,mismatchDescription);
		}
		return matches;
	}

	@Override
	public void describeTo(Description description) {
		description.appendText("output with size: ");
		sizeMatcher.describeTo(description);
	}

	public static <T> OutputWithSize<T> outputWithSize(Matcher<? super Integer> sizeMatcher) {
		return new OutputWithSize<T>(sizeMatcher);
	}

	public static <T> OutputWithSize<T> outputWithSize(int size) {
		return  OutputWithSize.<T>outputWithSize(IsEqual.equalTo(size));
	}
}
