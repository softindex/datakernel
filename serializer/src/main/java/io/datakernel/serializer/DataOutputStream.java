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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public final class DataOutputStream implements Closeable {
	private static final SerializeSizeException MAX_SIZE_EXCEPTION = new SerializeSizeException();
	public static final int DEFAULT_BUFFER_SIZE = 65536;

	public static final int MAX_SIZE_127 = 1; // (1 << (1 * 7)) - 1
	public static final int MAX_SIZE_16K = 2; // (1 << (2 * 7)) - 1
	public static final int MAX_SIZE_2M = 3; // (1 << (3 * 7)) - 1

	private OutputStream outputStream;

	private final SerializationOutputBuffer outputBuffer = new SerializationOutputBuffer();
	protected byte[] buf;
	protected int pos;

	private int estimatedMessageSize = 1;

	public DataOutputStream(OutputStream output) {
		this(output, DEFAULT_BUFFER_SIZE);
	}

	public DataOutputStream(OutputStream outputStream, int bufferSize) {
		this.outputStream = outputStream;
		allocateBuffer(bufferSize);
	}

	public void changeOutputStream(OutputStream outputStream) throws IOException {
		if (this.outputStream != null) {
			flush();
			this.outputStream.close();
		}
		this.outputStream = outputStream;
	}

	private void allocateBuffer(int size) {
		buf = new byte[size];
		pos = 0;
		outputBuffer.set(buf, 0);
	}

	private void doEnsureSize(int size) throws IOException {
		doFlush();
		if (buf.length < size) {
			allocateBuffer(size);
		} else {
			pos = 0;
		}
	}

	private void ensureSize(int size) throws IOException {
		if (buf.length - pos < size) {
			doEnsureSize(size);
		}
	}

	private void doFlush() throws IOException {
		if (pos != 0) {
			outputStream.write(buf, 0, pos);
			pos = 0;
		}
	}

	public void flush() throws IOException {
		doFlush();
		outputStream.flush();
	}

	@Override
	public void close() throws IOException {
		flush();
		outputStream.close();
	}

	private static int headerSize(int value) {
		if ((value & 0xffffffff << 7) == 0) return 1;
		if ((value & 0xffffffff << 14) == 0) return 2;
		assert (value & 0xffffffff << 21) == 0;
		return 3;
	}

	private static void writeSize(byte[] buf, int pos, int size, int headerSize) {
		if (headerSize == 1) {
			buf[pos] = (byte) size;
		} else {
			buf[pos] = (byte) (size | 0x80);
			size >>>= 7;

			if (headerSize == 2) {
				buf[pos + 1] = (byte) size;
			} else {
				buf[pos + 1] = (byte) (size | 0x80);
				size >>>= 7;

				assert headerSize == 3;

				buf[pos + 2] = (byte) size;
			}
		}
	}

	public <T> void serialize(BufferSerializer<T> serializer, T value, int headerSize) throws IOException, SerializeException {
		int positionBegin;
		int positionItem;
		for (; ; ) {
			ensureSize(headerSize + estimatedMessageSize);
			positionBegin = pos;
			positionItem = positionBegin + headerSize;
			outputBuffer.position(positionItem);
			try {
				serializer.serialize(outputBuffer, value);
			} catch (ArrayIndexOutOfBoundsException e) {
				int messageSize = buf.length - positionItem;
				estimatedMessageSize = messageSize + 1 + (messageSize >>> 1);
				continue;
			} catch (Exception e) {
				throw new SerializeException(e);
			}
			break;
		}
		int positionEnd = outputBuffer.position();
		int messageSize = positionEnd - positionItem;
		if (messageSize >= 1 << headerSize * 7) {
			throw MAX_SIZE_EXCEPTION;
		}
		writeSize(buf, positionBegin, messageSize, headerSize);
		pos = positionEnd;
		messageSize += messageSize >>> 2;
		if (messageSize > estimatedMessageSize)
			estimatedMessageSize = messageSize;
		else
			estimatedMessageSize -= estimatedMessageSize >>> 8;
	}

	public void write(byte[] b) throws IOException {
		ensureSize(b.length);
		pos = SerializerUtils.write(b, buf, pos);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		ensureSize(len);
		pos = SerializerUtils.write(b, off, buf, pos, len);
	}

	public void writeBoolean(boolean v) throws IOException {
		ensureSize(1);
		pos = SerializerUtils.writeBoolean(buf, pos, v);
	}

	public void writeByte(byte v) throws IOException {
		ensureSize(1);
		pos = SerializerUtils.writeByte(buf, pos, v);
	}

	public void writeShort(short v) throws IOException {
		ensureSize(2);
		pos = SerializerUtils.writeShort(buf, pos, v);
	}

	public void writeInt(int v) throws IOException {
		ensureSize(4);
		pos = SerializerUtils.writeInt(buf, pos, v);
	}

	public void writeLong(long v) throws IOException {
		ensureSize(8);
		pos = SerializerUtils.writeLong(buf, pos, v);
	}

	public void writeVarInt(int v) throws IOException {
		ensureSize(5);
		pos = SerializerUtils.writeVarInt(buf, pos, v);
	}

	public void writeVarLong(long v) throws IOException {
		ensureSize(9);
		pos = SerializerUtils.writeVarLong(buf, pos, v);
	}

	public void writeFloat(float v) throws IOException {
		ensureSize(4);
		pos = SerializerUtils.writeFloat(buf, pos, v);
	}

	public void writeDouble(double v) throws IOException {
		ensureSize(8);
		pos = SerializerUtils.writeDouble(buf, pos, v);
	}

	public void writeChar(char v) throws IOException {
		ensureSize(2);
		pos = SerializerUtils.writeChar(buf, pos, v);
	}

	public void writeUTF8(String s) throws IOException {
		ensureSize(5 + s.length() * 3);
		pos = SerializerUtils.writeJavaUTF8(buf, pos, s);
	}

	public void writeIso88591(String s) throws IOException {
		ensureSize(5 + s.length() * 3);
		pos = SerializerUtils.writeIso88591(buf, pos, s);
	}

	public final void writeUTF16(String s) throws IOException {
		ensureSize(5 + s.length() * 2);
		pos = SerializerUtils.writeUTF16(buf, pos, s);
	}

}
