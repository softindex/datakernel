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

public final class DeserializeInputStream<T> implements ObjectReader<T> {
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	public static final int MAX_HEADER_BYTES = 3;

	private InputStream inputStream;
	private final BufferSerializer<T> bufferSerializer;
	private final SerializationInputBuffer inputBuffer = new SerializationInputBuffer();
	private final int maxMessageSize;
	private byte[] buffer;
	private int off;

	private int dataSize;
	private int bufferLastPos;
	private int messageStart;

	public DeserializeInputStream(InputStream inputStream, BufferSerializer<T> bufferSerializer, int maxMessageSize) {
		this(inputStream, bufferSerializer, maxMessageSize, DEFAULT_BUFFER_SIZE);
	}

	public DeserializeInputStream(InputStream inputStream, BufferSerializer<T> bufferSerializer, int maxMessageSize, int bufferSize) {
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
		off = 0;
		bufferLastPos = 0;
		messageStart = 0;
	}

	@Override
	public T read() {
		for (; ; ) {
			int readBytes;
			try {
				readBytes = inputStream.read(buffer, bufferLastPos, buffer.length - bufferLastPos);
				if (readBytes != -1) {
					bufferLastPos += readBytes;
				}
			} catch (IOException e) {
				return null;
			}

			if (readBytes == -1 && off == bufferLastPos) {
				return null;
			}

			if (dataSize == 0) {
				assert buffer.length >= MAX_HEADER_BYTES;
				// read message header
				if (bufferLastPos - off >= MAX_HEADER_BYTES) {
					int headerLen = tryReadSize(buffer, off);
					if (headerLen > MAX_HEADER_BYTES)
						throw new IllegalArgumentException("Parsed size length > MAX_HEADER_BYTES");
					readBytes -= headerLen;
					off += headerLen;
					messageStart = off;
					if (off + dataSize >= bufferLastPos) {
						replaceInStart();
						resizeBuffer(dataSize);
						continue;
					}
				} else {
					replaceInStart();
					int headerLen = tryReadSize(buffer, 0);
					if (headerLen > bufferLastPos) {
						dataSize = 0;
						continue;
					}
					off += headerLen;
					messageStart = off;
					continue;
				}
				if (dataSize > maxMessageSize)
					throw new IllegalArgumentException("Parsed data size > message size");
			}
			// read message body:
			if (readBytes >= dataSize || bufferLastPos - messageStart >= dataSize) {
				inputBuffer.set(buffer, off);
				T item = bufferSerializer.deserialize(inputBuffer);
				if ((inputBuffer.position() - off) != dataSize)
					throw new IllegalArgumentException("Deserialized size != parsed data size");
				off += dataSize;
				dataSize = 0;
				return item;
			} else {
				replaceInStart();
				resizeBuffer(dataSize);
			}
		}

	}

	private void replaceInStart() {
		if (off != 0) {
			System.arraycopy(buffer, off, buffer, 0, bufferLastPos - off);
			bufferLastPos -= off;
			if (messageStart != 0) {
				messageStart -= off;
			}
			off = 0;
		}
	}

	private void resizeBuffer(int readSize) {
		int newSize = readSize + readSize / 4;
		if (newSize > buffer.length) {
			byte[] bytes = new byte[newSize];
			System.arraycopy(buffer, 0, bytes, 0, bufferLastPos);
			buffer = bytes;
		}
	}

	private int tryReadSize(byte[] buf, int off) {
		byte b = buf[off];
		if (b >= 0) {
			dataSize = b;
			return 1;
		}

		dataSize = b & 0x7f;
		b = buf[off + 1];
		if (b >= 0) {
			dataSize |= (b << 7);
			return 2;
		}

		dataSize |= ((b & 0x7f) << 7);
		b = buf[off + 2];
		if (b >= 0) {
			dataSize |= (b << 14);
			return 3;
		}

		dataSize = Integer.MAX_VALUE;
		return Integer.MAX_VALUE;
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
		this.inputBuffer.set(null, 0);
	}
}
