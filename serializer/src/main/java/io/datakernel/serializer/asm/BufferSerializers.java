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
		public int serialize(SerializationOutputBuffer output, Byte item) {
			output.position(SerializationOutputBuffer.writeByte(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Byte deserialize(SerializationInputBuffer input) {
			return input.readByte();
		}
	};

	private static final BufferSerializer<byte[]> BYTES_SERIALIZER = new BufferSerializer<byte[]>() {
		@Override
		public int serialize(SerializationOutputBuffer output, byte[] item) {
			output.position(SerializationOutputBuffer.writeVarInt(output.array(), output.position(), item.length));
			output.position(SerializationOutputBuffer.write(item, output.array(), output.position()));
			return output.position();
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
		public int serialize(SerializationOutputBuffer output, Short item) {
			output.position(SerializationOutputBuffer.writeShort(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Short deserialize(SerializationInputBuffer input) {
			return input.readShort();
		}
	};

	private static final BufferSerializer<Integer> INT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Integer item) {
			output.position(SerializationOutputBuffer.writeInt(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Integer deserialize(SerializationInputBuffer input) {
			return input.readInt();
		}
	};

	private static final BufferSerializer<Integer> VARINT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Integer item) {
			output.position(SerializationOutputBuffer.writeVarInt(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Integer deserialize(SerializationInputBuffer input) {
			return input.readVarInt();
		}
	};

	private static final BufferSerializer<Integer> VARINT_ZIGZAG_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Integer item) {
			output.position(SerializationOutputBuffer.writeVarInt(output.array(), output.position(), (item << 1) ^ (item >> 31)));
			return output.position();
		}

		@Override
		public Integer deserialize(SerializationInputBuffer input) {
			int n = input.readVarInt();
			return (n >>> 1) ^ -(n & 1);
		}
	};

	private static final BufferSerializer<Long> LONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Long item) {
			output.position(SerializationOutputBuffer.writeLong(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Long deserialize(SerializationInputBuffer input) {
			return input.readLong();
		}
	};

	private static final BufferSerializer<Long> VARLONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Long item) {
			output.position(SerializationOutputBuffer.writeVarLong(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Long deserialize(SerializationInputBuffer input) {
			return input.readVarLong();
		}
	};

	private static final BufferSerializer<Long> VARLONG_ZIGZAG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Long item) {
			output.position(SerializationOutputBuffer.writeVarLong(output.array(), output.position(), (item << 1) ^ (item >> 63)));
			return output.position();
		}

		@Override
		public Long deserialize(SerializationInputBuffer input) {
			long n = input.readVarLong();
			return (n >>> 1) ^ -(n & 1);
		}
	};

	private static final BufferSerializer<Float> FLOAT_SERIALIZER = new BufferSerializer<Float>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Float item) {
			output.position(SerializationOutputBuffer.writeFloat(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Float deserialize(SerializationInputBuffer input) {
			return input.readFloat();
		}
	};

	private static final BufferSerializer<Double> DOUBLE_SERIALIZER = new BufferSerializer<Double>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Double item) {
			output.position(SerializationOutputBuffer.writeDouble(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Double deserialize(SerializationInputBuffer input) {
			return input.readDouble();
		}
	};

	private static final BufferSerializer<Character> CHAR_SERIALIZER = new BufferSerializer<Character>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Character item) {
			output.position(SerializationOutputBuffer.writeChar(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Character deserialize(SerializationInputBuffer input) {
			return input.readChar();
		}
	};

	private static final BufferSerializer<String> UTF8_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public int serialize(SerializationOutputBuffer output, String item) {
			output.position(SerializationOutputBuffer.writeUTF8(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public String deserialize(SerializationInputBuffer input) {
			return input.readUTF8();
		}
	};

	private static final BufferSerializer<String> UTF16_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public int serialize(SerializationOutputBuffer output, String item) {
			output.position(SerializationOutputBuffer.writeUTF16(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public String deserialize(SerializationInputBuffer input) {
			return input.readUTF16();
		}
	};

	private static final BufferSerializer<Boolean> BOOLEAN_SERIALIZER = new BufferSerializer<Boolean>() {
		@Override
		public int serialize(SerializationOutputBuffer output, Boolean item) {
			output.position(SerializationOutputBuffer.writeBoolean(output.array(), output.position(), item));
			return output.position();
		}

		@Override
		public Boolean deserialize(SerializationInputBuffer input) {
			return input.readBoolean();
		}
	};

	private static final BufferSerializer<String> ISO_8859_1_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public int serialize(SerializationOutputBuffer output, String item) {
			output.position(SerializationOutputBuffer.writeIso88591(output.array(), output.position(), item));
			return output.position();
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
