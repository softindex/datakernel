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

import static java.lang.Math.min;

public final class DeserializeInputStream<T> implements ObjectReader<T> {
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	public static final int MAX_HEADER_BYTES = 3;

	private final InputStream inputStream;
	private final BufferSerializer<T> bufferSerializer;
	private final SerializationInputBuffer inputBuffer = new SerializationInputBuffer();
	private final int maxMessageSize;
	private byte[] buffer;
	private byte[] buf;
	private int off;
	private int readBytes;

	private int dataSize;
	private int bufferPos;

	public DeserializeInputStream(InputStream inputStream, BufferSerializer<T> bufferSerializer, int maxMessageSize) {
		this(inputStream, bufferSerializer, maxMessageSize, DEFAULT_BUFFER_SIZE);
	}

	public DeserializeInputStream(InputStream inputStream, BufferSerializer<T> bufferSerializer, int maxMessageSize, int bufferSize) {
		this.inputStream = inputStream;
		this.bufferSerializer = bufferSerializer;
		this.buf = new byte[bufferSize];
		this.buffer = new byte[bufferSize];
		this.inputBuffer.set(buffer, 0);
		this.maxMessageSize = maxMessageSize;
	}

	@Override
	public T read() {
		for (; ; ) {
			if (readBytes == 0) {
				try {
					readBytes = inputStream.read(buf);
					off = 0;
				} catch (IOException e) {
					return null;
				}
			}
			if (readBytes == -1) {
				return null;
			}
			while (readBytes > 0) {
				if (dataSize == 0) {
					assert bufferPos < MAX_HEADER_BYTES;
					assert buffer.length >= MAX_HEADER_BYTES;
					// read message header:
					if (bufferPos == 0 && readBytes >= MAX_HEADER_BYTES) {
						int sizeLen = tryReadSize(buf, off);
						if (sizeLen > MAX_HEADER_BYTES)
							throw new IllegalArgumentException("Parsed size length > MAX_HEADER_BYTES");
						readBytes -= sizeLen;
						off += sizeLen;
						bufferPos = 0;
					} else {
						int readSize = min(readBytes, MAX_HEADER_BYTES - bufferPos);
						System.arraycopy(buf, off, buffer, bufferPos, readSize);
						readBytes -= readSize;
						off += readSize;
						bufferPos += readSize;
						int sizeLen = tryReadSize(buffer, 0);
						if (sizeLen > bufferPos) {
							// Read past last position - incomplete varint in buffer, waiting for more bytes
							dataSize = 0;
							break;
						}
						int unreadSize = bufferPos - sizeLen;
						readBytes += unreadSize;
						off -= unreadSize;
						bufferPos = 0;
					}
					if (dataSize > maxMessageSize)
						throw new IllegalArgumentException("Parsed data size > message size");
				}
				// read message body:
				if (bufferPos == 0 && readBytes >= dataSize) {
					inputBuffer.set(buf, off);
					T item = bufferSerializer.deserialize(inputBuffer);
					if ((inputBuffer.position() - off) != dataSize)
						throw new IllegalArgumentException("Deserialized size != parsed data size");
					readBytes -= dataSize;
					off += dataSize;
					bufferPos = 0;
					dataSize = 0;
					return item;
				} else {
					int readSize = min(readBytes, dataSize - bufferPos);
					copyIntoBuffer(buf, off, readSize);
					readBytes -= readSize;
					off += readSize;
					if (bufferPos != dataSize)
						break;
					inputBuffer.set(buffer, 0);
					T item = bufferSerializer.deserialize(inputBuffer);
					if (inputBuffer.position() != dataSize)
						throw new IllegalArgumentException("Deserialized size != parsed data size");
					bufferPos = 0;
					dataSize = 0;
					return item;
				}
			}
		}
	}

	private void growBuf(int newSize) {
		byte[] bytes = new byte[newSize];
		System.arraycopy(buffer, 0, bytes, 0, bufferPos);
		buffer = bytes;
		byte[] bufBytes = new byte[newSize];
		System.arraycopy(buf, 0, bufBytes, 0, buf.length);
		buf = bufBytes;
	}

	private void copyIntoBuffer(byte[] b, int off, int len) {
		if (buffer.length < bufferPos + len) {
			growBuf(bufferPos + len);
		}
		System.arraycopy(b, off, buffer, bufferPos, len);
		bufferPos += len;
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
	}
}
