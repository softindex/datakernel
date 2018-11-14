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
import io.datakernel.util.MemSize;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static java.lang.Math.max;

public final class DataInputStreamEx implements Closeable {
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(16);

	private InputStream inputStream;
	private ByteBuf buf = ByteBuf.empty();
	private final int defaultBufferSize;

	private char[] charArray = new char[128];

	private DataInputStreamEx(InputStream inputStream, int defaultBufferSize) {
		this.inputStream = inputStream;
		this.defaultBufferSize = defaultBufferSize;
	}

	public static DataInputStreamEx create(InputStream inputStream) {
		return new DataInputStreamEx(inputStream, DEFAULT_BUFFER_SIZE.toInt());
	}

	public static DataInputStreamEx create(InputStream inputStream, int bufferSize) {
		return new DataInputStreamEx(inputStream, bufferSize);
	}

	public static DataInputStreamEx create(InputStream inputStream, MemSize bufferSize) {
		return create(inputStream, bufferSize.toInt());
	}

	public void changeInputStream(InputStream inputStream) throws IOException {
		if (this.inputStream != null) {
			this.inputStream.close();
		}
		this.inputStream = inputStream;
	}

	private void doEnsureRead(int size) throws IOException {
		try {
			while (buf.readRemaining() < size) {
				buf = ByteBufPool.ensureWriteRemaining(buf, max(defaultBufferSize, buf.array().length), size);
				int bytesRead = inputStream.read(buf.array(), buf.writePosition(), buf.writeRemaining());
				if (bytesRead == -1)
					throw new IOException("Could not read message");
				buf.moveWritePosition(bytesRead);
			}
		} catch (IOException e) {
			recycleInternalBuf();
			throw e;
		}
	}

	private void ensureRead(int size) throws IOException {
		if (buf.readRemaining() < size) {
			doEnsureRead(size);
		}
	}

	private int readSize() throws IOException {
		int result;
		byte b = readByte();
		if (b >= 0) {
			result = b;
		} else {
			result = b & 0x7f;
			if ((b = readByte()) >= 0) {
				result |= b << 7;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = readByte()) >= 0) {
					result |= b << 14;
				} else {
					close();
					throw new IOException();
				}
			}
		}
		return result;
	}

	@Override
	public void close() throws IOException {
		recycleInternalBuf();
		inputStream.close();
	}

	public void recycleInternalBuf() {
		buf.recycle();
		buf = ByteBuf.empty();
	}

	public boolean isEndOfStream() throws IOException {
		if (buf.canRead()) {
			return false;
		} else {
			buf = ByteBufPool.ensureWriteRemaining(buf, max(defaultBufferSize, buf.array().length), 1);
			int bytesRead = inputStream.read(buf.array(), buf.writePosition(), buf.writeRemaining());
			if (bytesRead == -1) {
				recycleInternalBuf();
				return true;
			}
			buf.moveWritePosition(bytesRead);
			return false;
		}
	}

	public <T> T deserialize(BufferSerializer<T> serializer) throws IOException, DeserializeException {
		int messageSize = readSize();

		ensureRead(messageSize);

		int oldHead = buf.readPosition();
		T item;
		try {
			item = serializer.deserialize(buf);
		} catch (Exception e) {
			throw new DeserializeException(e);
		}
		if (buf.readPosition() - oldHead != messageSize) {
			throw new DeserializeException("Deserialized size != parsed data size");
		}
		return item;
	}

	private char[] ensureCharArray(int length) {
		if (charArray.length < length) {
			charArray = new char[length + (length >>> 2)];
		}
		return charArray;
	}

	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) throws IOException {
		ensureRead(len);
		buf.drainTo(b, off, len);
		return len;
	}

	public byte readByte() throws IOException {
		ensureRead(1);
		return buf.get();
	}

	public boolean readBoolean() throws IOException {
		return readByte() != 0;
	}

	public short readShort() throws IOException {
		ensureRead(2);
		short result = (short) (((buf.peek(0) & 0xFF) << 8) | (buf.peek(1) & 0xFF));
		buf.moveReadPosition(2);
		return result;
	}

	public int readInt() throws IOException {
		ensureRead(4);
		int result = ((buf.peek(0) & 0xFF) << 24)
				| ((buf.peek(1) & 0xFF) << 16)
				| ((buf.peek(2) & 0xFF) << 8)
				| (buf.peek(3) & 0xFF);
		buf.moveReadPosition(4);
		return result;
	}

	public long readLong() throws IOException {
		ensureRead(8);
		long result = ((long) buf.peek(0) << 56)
				| ((long) (buf.peek(1) & 0xFF) << 48)
				| ((long) (buf.peek(2) & 0xFF) << 40)
				| ((long) (buf.peek(3) & 0xFF) << 32)
				| ((long) (buf.peek(4) & 0xFF) << 24)
				| ((buf.peek(5) & 0xFF) << 16)
				| ((buf.peek(6) & 0xFF) << 8)
				| (buf.peek(7) & 0xFF);

		buf.moveReadPosition(8);
		return result;
	}

	public int readVarInt() throws IOException, DeserializeException {
		int result;
		byte b = readByte();
		if (b >= 0) {
			result = b;
		} else {
			result = b & 0x7f;
			if ((b = readByte()) >= 0) {
				result |= b << 7;
			} else {
				result |= (b & 0x7f) << 7;
				if ((b = readByte()) >= 0) {
					result |= b << 14;
				} else {
					result |= (b & 0x7f) << 14;
					if ((b = readByte()) >= 0) {
						result |= b << 21;
					} else {
						result |= (b & 0x7f) << 21;
						if ((b = readByte()) >= 0) {
							result |= b << 28;
						} else {
							recycleInternalBuf();
							throw new DeserializeException();
						}
					}
				}
			}
		}
		return result;
	}

	public long readVarLong() throws IOException, DeserializeException {
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			byte b = readByte();
			result |= (long) (b & 0x7F) << offset;
			if ((b & 0x80) == 0)
				return result;
		}
		recycleInternalBuf();
		throw new DeserializeException();
	}

	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	public char readChar() throws IOException {
		ensureRead(2);
		char c = (char) (((buf.peek(0) & 0xFF) << 8) | (buf.peek(1) & 0xFF));
		buf.moveReadPosition(2);
		return c;
	}

	public String readUTF8() throws IOException, DeserializeException {
		int length = readVarInt();
		if (length == 0)
			return "";
		ensureRead(length);
		buf.moveReadPosition(length);

		return new String(buf.array(), buf.readPosition() - length, length, StandardCharsets.UTF_8);
	}

	public String readIso88591() throws IOException, DeserializeException {
		int length = readVarInt();
		if (length == 0)
			return "";
		ensureRead(length);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			int c = readByte() & 0xff;
			chars[i] = (char) c;
		}
		return new String(chars, 0, length);
	}

	public String readUTF16() throws IOException, DeserializeException {
		int length = readVarInt();
		if (length == 0)
			return "";
		ensureRead(length * 2);

		char[] chars = ensureCharArray(length);
		for (int i = 0; i < length; i++) {
			byte b1 = buf.get();
			byte b2 = buf.get();
			chars[i] = (char) (((b1 & 0xFF) << 8) + (b2 & 0xFF));
		}
		return new String(chars, 0, length);
	}

}
