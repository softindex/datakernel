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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.SerializationUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public final class DataOutputStreamEx implements Closeable {
	private static final SerializeException SIZE_EXCEPTION = new SerializeException("Message size of out range");
	public static final int DEFAULT_BUFFER_SIZE = 65536;

	public static final int MAX_SIZE_127 = 1; // (1 << (1 * 7)) - 1
	public static final int MAX_SIZE_16K = 2; // (1 << (2 * 7)) - 1
	public static final int MAX_SIZE_2M = 3; // (1 << (3 * 7)) - 1

	private OutputStream outputStream;
	private ByteBuf buf;

	private int estimatedMessageSize = 1;

	private DataOutputStreamEx(OutputStream outputStream, int bufferSize) {
		this.outputStream = outputStream;
		this.buf = ByteBufPool.allocate(bufferSize);
	}

	public static DataOutputStreamEx create(OutputStream output) {
		return new DataOutputStreamEx(output, DEFAULT_BUFFER_SIZE);
	}

	public static DataOutputStreamEx create(OutputStream outputStream, int bufferSize) {
		return new DataOutputStreamEx(outputStream, bufferSize);
	}

	public void changeOutputStream(OutputStream outputStream) throws IOException {
		if (this.outputStream != null) {
			flush();
			this.outputStream.close();
		}
		this.outputStream = outputStream;
	}

	private void doEnsureSize(int size) throws IOException {
		// flush previous values before resize
		doFlush();
		if (buf.writeRemaining() < size) {
			buf.recycle();
			buf = ByteBufPool.allocate(size);
		}
	}

	private void ensureSize(int size) throws IOException {
		if (buf.writeRemaining() < size) {
			doEnsureSize(size);
		}
	}

	private void doFlush() throws IOException {
		if (buf.canRead()) {
			outputStream.write(buf.array(), buf.readPosition(), buf.readRemaining());
			buf.readPosition(0);
			buf.writePosition(0);
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
		buf.recycle();
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
			positionBegin = buf.writePosition();
			positionItem = positionBegin + headerSize;
			buf.writePosition(positionItem);
			try {
				serializer.serialize(buf, value);
			} catch (ArrayIndexOutOfBoundsException e) {
				int messageSize = buf.limit() - positionItem;
				buf.writePosition(positionBegin);
				estimatedMessageSize = messageSize + 1 + (messageSize >>> 1);
				continue;
			} catch (Exception e) {
				buf.writePosition(positionBegin);
				throw new SerializeException(e);
			}
			break;
		}
		int positionEnd = buf.writePosition();
		int messageSize = positionEnd - positionItem;
		if (messageSize >= 1 << headerSize * 7) {
			buf.writePosition(positionBegin);
			throw SIZE_EXCEPTION;
		}
		writeSize(buf.array(), positionBegin, messageSize, headerSize);
		messageSize += messageSize >>> 2;
		if (messageSize > estimatedMessageSize)
			estimatedMessageSize = messageSize;
		else
			estimatedMessageSize -= estimatedMessageSize >>> 8;
	}

	public void write(byte[] b) throws IOException {
		ensureSize(b.length);
		int newTail = SerializationUtils.write(buf.array(), buf.writePosition(), b);
		buf.writePosition(newTail);
	}

	public void write(byte[] b, int off, int len) throws IOException {
		ensureSize(len);
		int newTail = SerializationUtils.write(buf.array(), buf.writePosition(), b, off, len);
		buf.writePosition(newTail);
	}

	public void writeBoolean(boolean v) throws IOException {
		ensureSize(1);
		int newTail = SerializationUtils.writeBoolean(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeByte(byte v) throws IOException {
		ensureSize(1);
		int newTail = SerializationUtils.writeByte(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeShort(short v) throws IOException {
		ensureSize(2);
		int newTail = SerializationUtils.writeShort(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeInt(int v) throws IOException {
		ensureSize(4);
		int newTail = SerializationUtils.writeInt(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeLong(long v) throws IOException {
		ensureSize(8);
		int newTail = SerializationUtils.writeLong(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeVarInt(int v) throws IOException {
		ensureSize(5);
		int newTail = SerializationUtils.writeVarInt(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeVarLong(long v) throws IOException {
		ensureSize(9);
		int newTail = SerializationUtils.writeVarLong(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeFloat(float v) throws IOException {
		ensureSize(4);
		int newTail = SerializationUtils.writeFloat(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeDouble(double v) throws IOException {
		ensureSize(8);
		int newTail = SerializationUtils.writeDouble(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeChar(char v) throws IOException {
		ensureSize(2);
		int newTail = SerializationUtils.writeChar(buf.array(), buf.writePosition(), v);
		buf.writePosition(newTail);
	}

	public void writeUTF8(String s) throws IOException {
		ensureSize(5 + s.length() * 3);
		int newTail = SerializationUtils.writeJavaUTF8(buf.array(), buf.writePosition(), s);
		buf.writePosition(newTail);
	}

	public void writeIso88591(String s) throws IOException {
		ensureSize(5 + s.length() * 3);
		int newTail = SerializationUtils.writeIso88591(buf.array(), buf.writePosition(), s);
		buf.writePosition(newTail);
	}

	public final void writeUTF16(String s) throws IOException {
		ensureSize(5 + s.length() * 2);
		int newTail = SerializationUtils.writeUTF16(buf.array(), buf.writePosition(), s);
		buf.writePosition(newTail);
	}

}
