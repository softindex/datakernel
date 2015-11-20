/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.serializer;

import java.io.IOException;
import java.io.OutputStream;

import static java.lang.Math.max;

public final class SerializeOutputStream<T> implements ObjectWriter<T> {
	private static final ArrayIndexOutOfBoundsException OUT_OF_BOUNDS_EXCEPTION = new ArrayIndexOutOfBoundsException();
	public static final int DEFAULT_BUFFER_SIZE = 1 << 20;

	public static final int MAX_SIZE_1_BYTE = 127; // (1 << (1 * 7)) - 1
	public static final int MAX_SIZE_2_BYTE = 16383; // (1 << (2 * 7)) - 1
	public static final int MAX_SIZE_3_BYTE = 2097151; // (1 << (3 * 7)) - 1
	public static final int MAX_SIZE = MAX_SIZE_3_BYTE;

	private static final int MAX_HEADER_BYTES = 3;

	private final BufferSerializer<T> serializer;
	private OutputStream outputStream;

	private final SerializationOutputBuffer outputBuffer = new SerializationOutputBuffer();

	private final boolean skipSerializationErrors;
	private final int maxMessageSize;
	private final int headerSize;
	private final int bufferSize;
	private int estimatedMessageSize;

	public SerializeOutputStream(OutputStream output, BufferSerializer<T> serializer, int maxMessageSize, boolean skipSerializationErrors) {
		this(output, serializer, maxMessageSize, DEFAULT_BUFFER_SIZE, skipSerializationErrors);
	}

	public SerializeOutputStream(OutputStream outputStream, BufferSerializer<T> serializer, int maxMessageSize, int bufferSize, boolean skipSerializationErrors) {
		this.serializer = serializer;
		this.bufferSize = bufferSize;
		this.outputStream = outputStream;
		this.maxMessageSize = maxMessageSize;
		this.headerSize = varint32Size(maxMessageSize);
		this.estimatedMessageSize = 1;
		this.skipSerializationErrors = skipSerializationErrors;
		allocateBuffer();
	}

	public void changeOutputStream(OutputStream outputStream) throws IOException {
		if (this.outputStream != null) {
			this.outputStream.close();
		}
		this.outputStream = outputStream;
	}

	private int varint32Size(int value) {
		if ((value & 0xffffffff << 7) == 0) return 1;
		if ((value & 0xffffffff << 14) == 0) return 2;
		if ((value & 0xffffffff << 21) == 0) return 3;
		if ((value & 0xffffffff << 28) == 0) return 4;
		return 5;
	}

	private void writeSize(byte[] buf, int pos, int size) {
		if (headerSize == 1) {
			buf[pos] = (byte) size;
			return;
		}

		buf[pos] = (byte) ((size & 0x7F) | 0x80);
		size >>>= 7;
		if (headerSize == 2) {
			buf[pos + 1] = (byte) size;
			return;
		}

		assert headerSize == 3;

		buf[pos + 1] = (byte) ((size & 0x7F) | 0x80);
		size >>>= 7;
		buf[pos + 2] = (byte) size;
	}

	private void ensureSize(int size) throws IOException {
		if (outputBuffer.remaining() < size) {
			write();
		}
	}

	private void allocateBuffer() {
		int max = max(bufferSize, headerSize + estimatedMessageSize);
		if (outputBuffer.array() == null || max > outputBuffer.array().length) {
			outputBuffer.set(new byte[max], 0);
		} else {
			outputBuffer.position(0);
		}
	}

	@Override
	public void write(T value) throws IOException {
		for (; ; ) {
			int positionBegin = outputBuffer.position();
			int positionItem = positionBegin + headerSize;
			try {
				ensureSize(headerSize + estimatedMessageSize);
				outputBuffer.position(positionItem);
				serializer.serialize(outputBuffer, value);
				int positionEnd = outputBuffer.position();
				int messageSize = positionEnd - positionItem;
				assert messageSize != 0;
				if (messageSize > maxMessageSize) {
					handleSerializationError(OUT_OF_BOUNDS_EXCEPTION);
					return;
				}
				writeSize(outputBuffer.array(), positionBegin, messageSize);
				messageSize += messageSize >>> 2;
				if (messageSize > estimatedMessageSize)
					estimatedMessageSize = messageSize;
				else
					estimatedMessageSize -= estimatedMessageSize >>> 8;
				break;
			} catch (ArrayIndexOutOfBoundsException e) {
				outputBuffer.position(positionBegin);
				int messageSize = outputBuffer.array().length - positionItem;
				if (messageSize >= maxMessageSize) {
					handleSerializationError(e);
					return;
				}
				estimatedMessageSize = messageSize + 1 + (messageSize >>> 1);
			} catch (Exception e) {
				handleSerializationError(e);
				return;
			}
		}

		write();
	}

	private void handleSerializationError(Exception e) {
		if (!skipSerializationErrors) {
			throw new RuntimeException(e);
		}
	}

	public void write() throws IOException {
		int size = outputBuffer.position();
		if (size != 0) {
			outputStream.write(outputBuffer.array(), 0, size);
		}
		allocateBuffer();
	}

	@Override
	public void flush() throws IOException {
		outputStream.flush();
	}

	@Override
	public void close() throws IOException {
		outputStream.close();
	}
}
