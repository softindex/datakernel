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
import java.io.InputStream;

public final class InputStreamDeserializer<T> implements BlockingObjectReader<T> {
	public static final int DEFAULT_BUFFER_SIZE = 65536;

	private InputStream inputStream;
	private final BufferSerializer<T> bufferSerializer;
	private final SerializationInputBuffer inputBuffer = new SerializationInputBuffer();
	private final int maxMessageSize;
	private byte[] buffer;
	private int pos;
	private int limit;

	public InputStreamDeserializer(InputStream inputStream, BufferSerializer<T> bufferSerializer, int maxMessageSize) {
		this(inputStream, bufferSerializer, maxMessageSize, DEFAULT_BUFFER_SIZE);
	}

	public InputStreamDeserializer(InputStream inputStream, BufferSerializer<T> bufferSerializer, int maxMessageSize, int bufferSize) {
		this.inputStream = inputStream;
		this.bufferSerializer = bufferSerializer;
		this.buffer = new byte[bufferSize];
		this.inputBuffer.set(buffer, 0);
		this.maxMessageSize = maxMessageSize;
	}

	public void changeInputStream(InputStream inputStream) throws IOException {
		if (this.inputStream != null) {
			this.inputStream.close();
		}
		this.inputStream = inputStream;
	}

	private void ensureReadSize(int size) {
		if (buffer.length - pos >= size) {
			return;
		}
		byte[] newBuffer = buffer.length >= size ? buffer : new byte[Math.max(buffer.length, size + size / 2)];
		System.arraycopy(buffer, pos, newBuffer, 0, limit - pos);
		buffer = newBuffer;
		limit -= pos;
		pos = 0;
		inputBuffer.set(buffer, 0);
	}

	private void ensureRead(int size) throws IOException {
		while (limit - pos < size) {
			ensureReadSize(size);
			int bytesRead = inputStream.read(buffer, limit, buffer.length - limit);
			if (bytesRead == -1)
				throw new IOException("Could not read message");
			limit += bytesRead;
		}
	}

	private byte readByte() throws IOException {
		if (pos == limit) {
			ensureRead(1);
		}
		return buffer[pos++];
	}

	private int readSize() throws IOException {
		for (int offset = 0, result = 0; offset < 32; offset += 7) {
			byte b = readByte();
			result |= (b & 0x7f) << offset;
			if ((b & 0x80) == 0)
				return result;
		}
		throw new IOException("Malformed size");
	}

	@Override
	public T read() throws IOException {
		if (pos == limit) {
			int bytesRead = inputStream.read(buffer, limit, buffer.length - limit);
			if (bytesRead == -1)
				return null;
			limit += bytesRead;
		}

		int messageSize = readSize();
		if (messageSize > maxMessageSize)
			throw new IOException("Message size > max message size");

		ensureRead(messageSize);

		inputBuffer.position(pos);
		T item = bufferSerializer.deserialize(inputBuffer);
		pos += messageSize;
		if (inputBuffer.position() != pos)
			throw new IOException("Deserialized size != parsed data size");
		return item;
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
		this.inputBuffer.set(null, 0);
	}
}
