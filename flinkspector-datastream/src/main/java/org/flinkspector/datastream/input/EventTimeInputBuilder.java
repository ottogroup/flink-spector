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

package org.flinkspector.datastream.input;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.flinkspector.datastream.input.time.Instant;
import org.flinkspector.datastream.input.time.Moment;
import org.flinkspector.datastream.input.time.TimeSpan;


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
	 * List of input containing StreamRecords and time shifts
	 */
	private ArrayList<Pair<StreamRecord<T>, Long>> input = new ArrayList<>();

	private Boolean flushWindows = false;

	/**
	 * Helper for adding records.
	 *
	 * @param record
	 */
	private void add(StreamRecord<T> record) {
		input.add(Pair.of(record, 0L));
	}

	/**
	 * Helper for adding records.
	 *
	 * @param record
	 */
	private void addWithShift(StreamRecord<T> record, Long shift) {
		input.add(Pair.of(record, shift));
	}

	private void addWithShift(T elem, Long timestamp, Long shift) {
		input.add(Pair.of(new StreamRecord<T>(elem, timestamp), shift));
	}

	private Long getLastTimestamp() {
		Pair<StreamRecord<T>, Long> lastRecord = input.get(input.size() - 1);
		return lastRecord.getLeft().getTimestamp() + lastRecord.getRight();
	}

	private EventTimeInputBuilder(StreamRecord<T> record) {
		add(record);
	}

	private EventTimeInputBuilder(StreamRecord<T> record, Long shift) {
		addWithShift(record, shift);
	}


	/**
	 * Create an {@link EventTimeInputBuilder} with the first record as input.
	 *
	 * @param record value
	 * @param <T>
	 * @return {@link EventTimeInputBuilder}
	 */
	public static <T> EventTimeInputBuilder<T> startWith(T record) {
		if (record == null) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(record, 0));
	}

	/**
	 * Create an {@link EventTimeInputBuilder} with the first record as input.
	 *
	 * @param record value
	 * @param <T>
	 * @return {@link EventTimeInputBuilder}
	 */
	public static <T> EventTimeInputBuilder<T> startWith(T record, long timeStamp) {
		if (record == null) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(record, timeStamp));
	}

	/**
	 * Create an {@link EventTimeInputBuilder} with the first record as input.
	 *
	 * @param record value
	 * @param <T>
	 * @return {@link EventTimeInputBuilder}
	 */
	public static <T> EventTimeInputBuilder<T> startWith(T record, Moment moment) {
		if (record == null) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(record, moment.getTimestamp(0)), moment.getShift());
	}

	/**
	 * Create an {@link EventTimeInputBuilder} with the first {@link StreamRecord} as input.
	 *
	 * @param streamRecord
	 * @param <T>
	 * @return {@link EventTimeInputBuilder}
	 */
	public static <T> EventTimeInputBuilder<T> startWith(StreamRecord<T> streamRecord) {
		if (streamRecord == null) {
			throw new IllegalArgumentException("Record has to be not null!");
		}
		return new EventTimeInputBuilder<T>(streamRecord);
	}

	/**
	 * Add an element with timestamp to the input.
	 *
	 * @param record
	 * @param timeStamp
	 * @return
	 */
	public EventTimeInputBuilder<T> emit(T record, long timeStamp) {
		if (timeStamp < 0) {
			throw new IllegalArgumentException("negative timestamp: " + timeStamp);
		}
		if (record == null) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		add(new StreamRecord<T>(record, timeStamp));
		return this;
	}

	/**
	 * Add an element with the timestamp of the previous record to the input.
	 *
	 * @param record
	 * @return
	 */
	public EventTimeInputBuilder<T> emit(T record) {
		if (record == null) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		add(new StreamRecord<T>(record, getLastTimestamp()));
		return this;
	}

	/**
	 * Add an element with an {@link TimeSpan} object,
	 * defining the time between the previous and the new record.
	 *
	 * @param record
	 * @param timeSpan {@link TimeSpan}
	 * @return
	 */
	public EventTimeInputBuilder<T> emit(T record, Moment timeSpan) {
		if (timeSpan == null) {
			throw new IllegalArgumentException("TimeBetween has to bo not null!");
		}
		long newTimeStamp = timeSpan.getTimestamp(getLastTimestamp());
		addWithShift(new StreamRecord<T>(record, newTimeStamp), timeSpan.getShift());
		return this;
	}

	/**
	 * Add a {@link StreamRecord} to the list of input.
	 *
	 * @param streamRecord
	 * @return
	 */
	public EventTimeInputBuilder<T> emit(StreamRecord<T> streamRecord) {
		if (streamRecord == null) {
			throw new IllegalArgumentException("Record has to be not null!");
		}
		emit(streamRecord.getValue(), streamRecord.getTimestamp());
		return this;
	}

	/**
	 * Repeats the last element.
	 *
	 * @param times number of times the input ist will be repeated.
	 */
	public EventTimeInputBuilder<T> emit(T elem, Moment moment, int times) {
		if (moment == null) {
			throw new IllegalArgumentException("TimeBetween has to bo not null!");
		}
		if (times < 1) {
			throw new IllegalArgumentException("Times has to be greater than 1.");
		}
		long ts = getLastTimestamp();
		for (int i = 0; i < times; i++) {

			ts = moment.getTimestamp(ts);
			addWithShift(elem, ts, moment.getShift());
		}
		return this;
	}

	private long calculateShiftDifference(Pair<StreamRecord<T>,Long> entry) {
		return entry.getLeft().getTimestamp() + entry.getRight();
	}

	/**
	 * Repeat the current input list.
	 * The time span between records in your already defined list will
	 * be kept.
	 *
	 * @param times    number of times the input ist will be repeated.
	 */
	public EventTimeInputBuilder<T> repeatAll(int times) {
		repeatAll(new Instant(), times);
		return this;
	}

	/**
	 * Repeat the current input list, after the defined span.
	 * The time span between records in your already defined list will
	 * be kept.
	 *
	 * @param timeSpan defining the time before and between repeating.
	 * @param times    number of times the input ist will be repeated.
	 */
	public EventTimeInputBuilder<T> repeatAll(TimeSpan timeSpan, int times) {
		long start = getLastTimestamp();
		List<Pair<StreamRecord<T>, Long>> toAppend = new ArrayList<>();
		for (int i = 0; i < times; i++) {
			toAppend.addAll(repeatInput(timeSpan.getTimestamp(0), start));
			start = calculateShiftDifference(toAppend.get(toAppend.size() - 1));
		}
		input.addAll(toAppend); //TODO change
		return this;
	}

	/**
	 * Causes the last timestamp to be MAX_VALUE.
	 */
	public EventTimeInputBuilder<T> flushOpenWindowsOnTermination() {
		flushWindows = true;
		return this;
	}

	/**
	 * Print the input list.
	 *
	 * @return
	 */
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (Pair<StreamRecord<T>, Long> r : input) {
			builder.append("value: " + r.getValue() +
					" timestamp: " + r.getLeft().getTimestamp() + "\n");
		}
		return builder.toString();
	}

	@Override
	public Boolean getFlushWindowsSetting() {
		return flushWindows;
	}

	@Override
	public List<StreamRecord<T>> getInput() {
		ArrayList<StreamRecord<T>> records = new ArrayList<>();
		for (Pair<StreamRecord<T>, Long> r : input) {
			records.add(r.getLeft());
		}
		return records;
	}

	private List<Pair<StreamRecord<T>, Long>> repeatInput(long time, long startTimeStamp) {

		List<Pair<StreamRecord<T>, Long>> append = new ArrayList<>();
		Iterator<Pair<StreamRecord<T>, Long>> it = input.iterator();
		long last = startTimeStamp;
		long delta = time;

		//first step
		Pair<StreamRecord<T>, Long> record = it.next();
		append.add(Pair.of(copyRecord(record.getLeft(), last + delta), 0L));
		long previous = record.getLeft().getTimestamp();
		long previousShift = record.getRight();
		last = last + delta;

		while (it.hasNext()) {
			record = it.next();
			delta = record.getLeft().getTimestamp() - previous;
			if(previousShift > 0 && record.getRight() > 0) {
				delta -= record.getRight();
			}
			if (last + delta < 0) {
				throw new UnsupportedOperationException("Negative timestamp: " + last + delta);
			}
			if(it.hasNext()) {
				append.add(Pair.of(copyRecord(record.getLeft(),
						last + delta), 0L));
			}else{
				append.add(Pair.of(copyRecord(record.getLeft(),
						last + delta), record.getRight()));
			}
			last = last + delta;
			previous = record.getLeft().getTimestamp();
			previousShift = record.getRight();
		}
		return append;
	}

	private StreamRecord<T> copyRecord(StreamRecord<T> record, Long timestamp) {
		return new StreamRecord<T>(record.getValue(), timestamp);
	}

}
