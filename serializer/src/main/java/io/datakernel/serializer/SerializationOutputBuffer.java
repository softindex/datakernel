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

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class SerializationOutputBuffer {
	protected byte[] buf;
	protected int pos;

	public SerializationOutputBuffer() {
	}

	public SerializationOutputBuffer(byte[] array) {
		this.buf = array;
		this.pos = 0;
	}

	public SerializationOutputBuffer(byte[] array, int position) {
		this.buf = array;
		this.pos = position;
	}

	public void set(byte[] buf, int pos) {
		this.buf = buf;
		this.pos = pos;
	}

	public byte[] array() {
		return buf;
	}

	public int position() {
		return pos;
	}

	public void position(int pos) {
		this.pos = pos;
	}

	public ByteBuffer getByteBuffer() {
		return ByteBuffer.wrap(buf, 0, pos);
	}

	public int remaining() {
		return buf.length - pos;
	}

	public void write(byte[] b) {
		pos = SerializerUtils.write(b, buf, pos);
	}

	public void write(byte[] b, int off, int len) {
		pos = SerializerUtils.write(b, off, buf, pos, len);
	}

	public void writeBoolean(boolean v) {
		pos = SerializerUtils.writeBoolean(buf, pos, v);
	}

	public void writeByte(byte v) {
		pos = SerializerUtils.writeByte(buf, pos, v);
	}

	public void writeChar(char v) {
		pos = SerializerUtils.writeChar(buf, pos, v);
	}

	public void writeDouble(double v) {
		pos = SerializerUtils.writeDouble(buf, pos, v);
	}

	public void writeFloat(float v) {
		pos = SerializerUtils.writeFloat(buf, pos, v);
	}

	public void writeInt(int v) {
		pos = SerializerUtils.writeInt(buf, pos, v);
	}

	public void writeLong(long v) {
		pos = SerializerUtils.writeLong(buf, pos, v);
	}

	public void writeShort(short v) {
		pos = SerializerUtils.writeShort(buf, pos, v);
	}

	public void writeVarInt(int v) {
		pos = SerializerUtils.writeVarInt(buf, pos, v);
	}

	public void writeVarLong(long v) {
		pos = SerializerUtils.writeVarLong(buf, pos, v);
	}

	public void writeIso88591(String s) {
		pos = SerializerUtils.writeIso88591(buf, pos, s);
	}

	public void writeNullableIso88591(String s) {
		int length = s.length();
		pos = SerializerUtils.writeNullableIso88591(buf, pos, s);
	}

	public void writeJavaUTF8(String s) {
		pos = SerializerUtils.writeJavaUTF8(buf, pos, s);
	}

	public void writeNullableJavaUTF8(String s) {
		pos = SerializerUtils.writeNullableJavaUTF8(buf, pos, s);
	}

	private void writeWithLengthNullable(byte[] bytes) {
		pos = SerializerUtils.writeWithLengthNullable(buf, pos, bytes);
	}

	public void writeUTF8(String s) {
		pos = SerializerUtils.writeUTF8(buf, pos, s);
	}

	public void writeNullableUTF8(String s) {
		pos = SerializerUtils.writeNullableUTF8(buf, pos, s);
	}

	private void writeUtfChar(int c) {
		pos = SerializerUtils.writeUtfChar(buf, pos, c);
	}

	public final void writeWithLength(byte[] bytes) {
		pos = SerializerUtils.writeWithLength(buf, pos, bytes);
	}

	public final void writeUTF16(String s) {
		pos = SerializerUtils.writeUTF16(buf, pos, s);
	}

	public final void writeNullableUTF16(String s) {
		pos = SerializerUtils.writeNullableUTF16(buf, pos, s);
	}

	@Override
	public String toString() {
		return "[pos: " + pos + ", buf" + (buf.length < 100 ? ": " + Arrays.toString(buf) : " size " + buf.length) + "]";
	}
}
