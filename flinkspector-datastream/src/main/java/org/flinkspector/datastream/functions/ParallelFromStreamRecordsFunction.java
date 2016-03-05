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

package org.flinkspector.datastream.functions;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.flinkspector.datastream.util.InputUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A stream source function that returns a sequence of elements with given TimeStamps.
 *
 * <p>Upon construction, this source function serializes the elements using Flink's type information.
 * That way, any object transport using Java serialization will not be affected by the serializability
 * of the elements.</p>
 *
 * @param <T> The type of elements returned by this function.
 */
public class ParallelFromStreamRecordsFunction<T> extends RichParallelSourceFunction<T>
		implements CheckpointedAsynchronously<Integer> {

	private static final long serialVersionUID = 1L;

	/**
	 * The (de)serializer to be used for the data elements
	 */
	private final TypeSerializer<StreamRecord<T>> serializer;

	/**
	 * The actual data elements, in serialized form
	 */
	private final byte[] elementsSerialized;

	/**
	 * The number of serialized elements
	 */
	private final int numElements;

	/**
	 * The number of elements emitted already
	 */
	private volatile int numElementsEmitted;

	/**
	 * The number of elements to skip initially
	 */
	private volatile int numElementsToSkip;

	/**
	 * Flag to make the source cancelable
	 */
	private volatile boolean isRunning = true;

	public ParallelFromStreamRecordsFunction(TypeSerializer<StreamRecord<T>> serializer,
											Iterable<StreamRecord<T>> input) throws IOException {
		this.serializer = serializer;
		elementsSerialized = serializeOutput(input, serializer).toByteArray();
		numElements = Iterables.size(input);
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {

		List<Long> watermarks;
		List<StreamRecord<T>> outputSplit;

		//-----------------------------------------------------------------
		// Split input and calculate watermarks
		//-----------------------------------------------------------------

		int numberOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		int indexOfThisSubTask = getRuntimeContext().getIndexOfThisSubtask();

		ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
		final DataInputView input = new DataInputViewStreamWrapper(new DataInputStream(bais));

		List<StreamRecord<T>> output = new ArrayList<>();
		//materialize input
		for (int i = 0; i < numElements; i++) {
			output.add(serializer.deserialize(input));
		}

		//split output if parallelism is greater than 1
		if(numberOfSubTasks > 1) {
			if (numberOfSubTasks > output.size()) {
				throw new IllegalStateException("Parallelism of source is higher than the" +
						" maximum number of parallel sources");
			}

			outputSplit = InputUtil.splitList(output,
					indexOfThisSubTask,
					numberOfSubTasks);
		}else{
			outputSplit = output;
		}
		//calculate watermarks
		watermarks = InputUtil.calculateWatermarks(outputSplit);
		// if we restored from a checkpoint and need to skip elements, skip them now.
		this.numElementsEmitted = this.numElementsToSkip;

		//-----------------------------------------------------------------
		// Emit elements
		//-----------------------------------------------------------------

		final Object lock = ctx.getCheckpointLock();

		while (isRunning && numElementsEmitted < outputSplit.size()) {
			StreamRecord<T> next = outputSplit.get(numElementsEmitted);
			synchronized (lock) {
				//logic where to put?
				ctx.collectWithTimestamp(next.getValue(), next.getTimestamp());
				Long watermark = watermarks.get(numElementsEmitted);
				if (watermark > 0) {
					ctx.emitWatermark(new Watermark(watermark));
				}
				numElementsEmitted++;
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	/**
	 * Gets the number of elements produced in total by this function.
	 *
	 * @return The number of elements produced in total.
	 */
	public int getNumElements() {
		return numElements;
	}

	/**
	 * Gets the number of elements emitted so far.
	 *
	 * @return The number of elements emitted so far.
	 */
	public int getNumElementsEmitted() {
		return numElementsEmitted;
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public Integer snapshotState(long checkpointId, long checkpointTimestamp) {
		return this.numElementsEmitted;
	}

	@Override
	public void restoreState(Integer state) {
		this.numElementsToSkip = state;
	}

	public static <OUT> void checkCollection(Iterable<OUT> elements, Class<OUT> viewedAs) {
		Iterator i$ = elements.iterator();

		Object elem;
		do {
			if (!i$.hasNext()) {
				return;
			}

			elem = i$.next();
			if (elem == null) {
				throw new IllegalArgumentException("The collection contains a null element");
			}
		} while (viewedAs.isAssignableFrom(elem.getClass()));

		throw new IllegalArgumentException("The elements in the collection are not all subclasses of "
				+ viewedAs.getCanonicalName());
	}

	private static <T> ByteArrayOutputStream serializeOutput(Iterable<StreamRecord<T>> elements,
	                                                        TypeSerializer<StreamRecord<T>> serializer) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(new DataOutputStream(baos));

		try {
			for (StreamRecord<T> element : elements) {
				serializer.serialize(element, wrapper);
			}
		}
		catch (Exception e) {
			throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
		}
		return baos;
	}
}
