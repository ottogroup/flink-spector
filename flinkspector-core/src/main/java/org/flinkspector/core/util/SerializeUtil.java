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

package org.flinkspector.core.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.*;

//TODO org.apache.flink.streaming.test this class

/**
 * Helper to serialize data using the {@link TypeSerializer}
 */
public class SerializeUtil {

	/**
	 * Serialize an object using a TypeSerializer.
	 *
	 * @param object     to serialize
	 * @param serializer to use;
	 * @param <IN>       type of the object
	 * @return serialized object
	 * @throws IOException
	 */
	public static <IN> byte[] serialize(IN object, TypeSerializer<IN> serializer) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(new DataOutputStream(baos));
		serializer.serialize(object, wrapper);
		return baos.toByteArray();
	}

	/**
	 * Deserializes an byte array using the provided TypeSerializer.
	 *
	 * @param bytes      byte array containing the serialized object
	 * @param serializer to use
	 * @param <OUT>      type of the serialized object
	 * @return deserialized object
	 * @throws IOException
	 */
	public static <OUT> OUT deserialize(byte[] bytes, TypeSerializer<OUT> serializer) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		final DataInputView input = new DataInputViewStreamWrapper(new DataInputStream(bais));
		try {
			return serializer.deserialize(input);
		} catch (EOFException e) {
			throw new EOFException("Could not des: " + new String(bytes));
		}
	}

	/**
	 * Deserializes a serialized TypeSerializer
	 *
	 * @param bytes serialized TypeSerializer
	 * @param <T>   type of the serializer
	 * @return deserialized TypeSerializer
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked typecast")
	public static <T> TypeSerializer<T> deserialize(byte[] bytes) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);

		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			return (TypeSerializer<T>) in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not deserialize class");
		} finally {
			try {
				bis.close();
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
		}
	}

	/**
	 * Serialize an TypeSerializer
	 *
	 * @param serializer to serialize
	 * @param <T>        type of the serializer
	 * @return serialized TypeSerializer
	 * @throws IOException
	 */
	public static <T> byte[] serialize(TypeSerializer<T> serializer) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(serializer);
			return bos.toByteArray();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}
	}


}
