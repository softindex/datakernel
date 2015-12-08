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

package io.datakernel.serializer.asm;

import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializationInputBuffer;
import io.datakernel.serializer.SerializationOutputBuffer;

public final class BufferSerializers {
	private BufferSerializers() {
	}

	private static final BufferSerializer<Byte> BYTE_SERIALIZER = new BufferSerializer<Byte>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Byte item) {
			output.writeByte(item);
		}

		@Override
		public Byte deserialize(SerializationInputBuffer input) {
			return input.readByte();
		}
	};

	private static final BufferSerializer<byte[]> BYTES_SERIALIZER = new BufferSerializer<byte[]>() {
		@Override
		public void serialize(SerializationOutputBuffer output, byte[] item) {
			output.writeVarInt(item.length);
			output.write(item);
		}

		@Override
		public byte[] deserialize(SerializationInputBuffer input) {
			int size = input.readVarInt();
			byte[] result = new byte[size];
			input.read(result);
			return result;
		}
	};

	private static final BufferSerializer<Short> SHORT_SERIALIZER = new BufferSerializer<Short>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Short item) {
			output.writeShort(item);
		}

		@Override
		public Short deserialize(SerializationInputBuffer input) {
			return input.readShort();
		}
	};

	private static final BufferSerializer<Integer> INT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Integer item) {
			output.writeInt(item);
		}

		@Override
		public Integer deserialize(SerializationInputBuffer input) {
			return input.readInt();
		}
	};

	private static final BufferSerializer<Integer> VARINT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Integer item) {
			output.writeVarInt(item);
		}

		@Override
		public Integer deserialize(SerializationInputBuffer input) {
			return input.readVarInt();
		}
	};

	private static final BufferSerializer<Integer> VARINT_ZIGZAG_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Integer item) {
			output.writeVarInt((item << 1) ^ (item >> 31));
		}

		@Override
		public Integer deserialize(SerializationInputBuffer input) {
			int n = input.readVarInt();
			return (n >>> 1) ^ -(n & 1);
		}
	};

	private static final BufferSerializer<Long> LONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Long item) {
			output.writeLong(item);
		}

		@Override
		public Long deserialize(SerializationInputBuffer input) {
			return input.readLong();
		}
	};

	private static final BufferSerializer<Long> VARLONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Long item) {
			output.writeVarLong(item);
		}

		@Override
		public Long deserialize(SerializationInputBuffer input) {
			return input.readVarLong();
		}
	};

	private static final BufferSerializer<Long> VARLONG_ZIGZAG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Long item) {
			output.writeVarLong((item << 1) ^ (item >> 63));
		}

		@Override
		public Long deserialize(SerializationInputBuffer input) {
			long n = input.readVarLong();
			return (n >>> 1) ^ -(n & 1);
		}
	};

	private static final BufferSerializer<Float> FLOAT_SERIALIZER = new BufferSerializer<Float>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Float item) {
			output.writeFloat(item);
		}

		@Override
		public Float deserialize(SerializationInputBuffer input) {
			return input.readFloat();
		}
	};

	private static final BufferSerializer<Double> DOUBLE_SERIALIZER = new BufferSerializer<Double>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Double item) {
			output.writeDouble(item);
		}

		@Override
		public Double deserialize(SerializationInputBuffer input) {
			return input.readDouble();
		}
	};

	private static final BufferSerializer<Character> CHAR_SERIALIZER = new BufferSerializer<Character>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Character item) {
			output.writeChar(item);
		}

		@Override
		public Character deserialize(SerializationInputBuffer input) {
			return input.readChar();
		}
	};

	private static final BufferSerializer<String> UTF8_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public void serialize(SerializationOutputBuffer output, String item) {
			output.writeUTF8(item);
		}

		@Override
		public String deserialize(SerializationInputBuffer input) {
			return input.readUTF8();
		}
	};

	private static final BufferSerializer<String> UTF16_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public void serialize(SerializationOutputBuffer output, String item) {
			output.writeUTF16(item);
		}

		@Override
		public String deserialize(SerializationInputBuffer input) {
			return input.readUTF16();
		}
	};

	private static final BufferSerializer<Boolean> BOOLEAN_SERIALIZER = new BufferSerializer<Boolean>() {
		@Override
		public void serialize(SerializationOutputBuffer output, Boolean item) {
			output.writeBoolean(item);
		}

		@Override
		public Boolean deserialize(SerializationInputBuffer input) {
			return input.readBoolean();
		}
	};

	private static final BufferSerializer<String> ISO_8859_1_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public void serialize(SerializationOutputBuffer output, String item) {
			output.writeIso88591(item);
		}

		@Override
		public String deserialize(SerializationInputBuffer input) {
			return input.readIso88591();
		}
	};

	public static BufferSerializer<Byte> byteSerializer() {
		return BYTE_SERIALIZER;
	}

	public static BufferSerializer<byte[]> bytesSerializer() {
		return BYTES_SERIALIZER;
	}

	public static BufferSerializer<Short> shortSerializer() {
		return SHORT_SERIALIZER;
	}

	public static BufferSerializer<Integer> intSerializer() {
		return INT_SERIALIZER;
	}

	public static BufferSerializer<Integer> varIntSerializer(boolean optimizePositive) {
		return optimizePositive ? VARINT_SERIALIZER : VARINT_ZIGZAG_SERIALIZER;
	}

	public static BufferSerializer<Long> longSerializer() {
		return LONG_SERIALIZER;
	}

	public static BufferSerializer<Long> varLongSerializer(boolean optimizePositive) {
		return optimizePositive ? VARLONG_SERIALIZER : VARLONG_ZIGZAG_SERIALIZER;
	}

	public static BufferSerializer<Float> floatSerializer() {
		return FLOAT_SERIALIZER;
	}

	public static BufferSerializer<Double> doubleSerializer() {
		return DOUBLE_SERIALIZER;
	}

	public static BufferSerializer<Character> charSerializer() {
		return CHAR_SERIALIZER;
	}

	public static BufferSerializer<String> stringSerializer() {
		return UTF8_SERIALIZER;
	}

	public static BufferSerializer<String> utf8Serializer() {
		return UTF8_SERIALIZER;
	}

	public static BufferSerializer<String> utf16Serializer() {
		return UTF16_SERIALIZER;
	}

	public static BufferSerializer<String> iso88591Serializer() { return ISO_8859_1_SERIALIZER; }
}
