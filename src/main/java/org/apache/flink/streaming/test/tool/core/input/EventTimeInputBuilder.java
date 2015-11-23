/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.core.input;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.test.tool.runtime.input.EventTimeInput;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Builder to define the input for a test
 * Offers multiple methods to generate input with the EventTime attached.
 *
 * @param <T> value type
 */
public class EventTimeInputBuilder<T> implements EventTimeInput<T> {

	/**
	 * List of input containing StreamRecords
	 */
	private ArrayList<StreamRecord<T>> input = new ArrayList<>();

	private EventTimeInputBuilder(StreamRecord<T> record) {
		input.add(record);
	}

	/**
	 * Adds the first record to the input
	 *
	 * @param elem value
	 * @param <T>
	 * @return
	 */
	public static <T> EventTimeInputBuilder<T> create(T elem) {
		if(elem == null ) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(elem, 0));
	}

	/**
	 * Adds the first StreamRecord to the input
	 *
	 * @param record
	 * @param <T>
	 * @return
	 */
	public static <T> EventTimeInputBuilder<T> create(StreamRecord<T> record) {
		if(record == null ) {
			throw new IllegalArgumentException("Record has to be not null!");
		}
		return new EventTimeInputBuilder<T>(record);
	}

	/**
	 * Add an element with timestamp to the input
	 *
	 * @param elem
	 * @param timeStamp
	 * @return
	 */
	public EventTimeInputBuilder<T> emit(T elem, long timeStamp) {
		if (timeStamp < 0) {
			throw new IllegalArgumentException("negative timestamp: " + timeStamp);
		}
		if(elem == null ) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		input.add(new StreamRecord<T>(elem, timeStamp));
		return this;
	}

	/**
	 * Add an element with an {@link After} object,
	 * defining the time between the previous and the new record.
	 *
	 * @param elem
	 * @param timeSpan {@link TimeSpan}
	 * @return
	 */
	public EventTimeInputBuilder<T> emit(T elem, TimeSpan timeSpan) {
		if(timeSpan == null) {
			throw new IllegalArgumentException("TimeBetween has to bo not null!");
		}
		long lastTimeStamp = input.get(input.size() - 1).getTimestamp();
		long newTimeStamp = lastTimeStamp + timeSpan.getTimeSpan();
		emit(elem, newTimeStamp);
		return this;
	}

	/**
	 * Add a {@link StreamRecord} to the list of input
	 *
	 * @param record
	 * @return
	 */
	public EventTimeInputBuilder<T> emit(StreamRecord<T> record) {
		if(record == null ) {
			throw new IllegalArgumentException("Record has to be not null!");
		}
		emit(record.getValue(),record.getTimestamp());
		return this;
	}

	/**
	 * Repeats the last element
	 * @param times    number of times the input ist will be repeated
	 */
	public EventTimeInputBuilder<T> emit(T elem, TimeSpan timeInterval, int times) {
		if(timeInterval == null) {
			throw new IllegalArgumentException("TimeBetween has to bo not null!");
		}
		if(times < 1) {
			throw new IllegalArgumentException("Times has to be greater than 1.");
		}
		long ts = input.get(input.size() - 1).getTimestamp();
		for (int i = 0; i < times; i++) {
			ts = ts + timeInterval.getTimeSpan();
			emit(elem, ts);
		}
		return this;
	}

	/**
	 * Repeat the current input list
	 *
	 * @param timeSpan defining the time before and between repeating
	 * @param times    number of times the input ist will be repeated
	 */
	public EventTimeInputBuilder<T> repeatAll(TimeSpan timeSpan, int times) {
		long start = input.get(input.size() - 1).getTimestamp();
		List<StreamRecord<T>> toAppend = new ArrayList<>();
		for (int i = 0; i < times; i++) {
			toAppend.addAll(repeatInput(timeSpan.getTimeSpan(), start));
			start = toAppend.get(toAppend.size() - 1).getTimestamp();
		}
		input.addAll(toAppend);
		return this;
	}

	/**
	 * Print the input list.
	 * @return
	 */
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (StreamRecord<T> r : input) {
			builder.append("value: " + r.getValue() + " timestamp: " + r.getTimestamp() + "\n");
		}
		return builder.toString();
	}

	@Override
	public List<StreamRecord<T>> getInput() {
		return input;
	}

	private List<StreamRecord<T>> repeatInput(long time, long startTimeStamp) {

		List<StreamRecord<T>> append = new ArrayList<>();
		Iterator<StreamRecord<T>> it = input.iterator();
		long last = startTimeStamp;
		long delta = time;

		//first step
		StreamRecord<T> record = it.next();
		append.add(new StreamRecord<T>(record.getValue(),
				last + delta));
		long previous = record.getTimestamp();
		last = last + delta;

		while (it.hasNext()) {
			record = it.next();
			delta = record.getTimestamp() - previous;
			if(last + delta < 0) {
				throw new UnsupportedOperationException("Negative timestamp: " + last + delta);
			}
			append.add(new StreamRecord<T>(record.getValue(),
					last + delta));
			last = last + delta;
			previous = record.getTimestamp();
		}
		return append;
	}
}
