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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.BufferSerializer;

import java.util.*;
import java.util.function.Supplier;

public final class BufferSerializers {
	private BufferSerializers() {
	}

	public static final BufferSerializer<Byte> BYTE_SERIALIZER = new BufferSerializer<Byte>() {
		@Override
		public void serialize(ByteBuf output, Byte item) {
			output.writeByte(item);
		}

		@Override
		public Byte deserialize(ByteBuf input) {
			return input.readByte();
		}
	};

	public static final BufferSerializer<byte[]> BYTES_SERIALIZER = new BufferSerializer<byte[]>() {
		@Override
		public void serialize(ByteBuf output, byte[] item) {
			output.writeVarInt(item.length);
			output.write(item);
		}

		@Override
		public byte[] deserialize(ByteBuf input) {
			int size = input.readVarInt();
			byte[] result = new byte[size];
			input.read(result);
			return result;
		}
	};

	public static final BufferSerializer<Short> SHORT_SERIALIZER = new BufferSerializer<Short>() {
		@Override
		public void serialize(ByteBuf output, Short item) {
			output.writeShort(item);
		}

		@Override
		public Short deserialize(ByteBuf input) {
			return input.readShort();
		}
	};

	public static final BufferSerializer<Integer> INT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public void serialize(ByteBuf output, Integer item) {
			output.writeInt(item);
		}

		@Override
		public Integer deserialize(ByteBuf input) {
			return input.readInt();
		}
	};

	public static final BufferSerializer<Integer> VARINT_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public void serialize(ByteBuf output, Integer item) {
			output.writeVarInt(item);
		}

		@Override
		public Integer deserialize(ByteBuf input) {
			return input.readVarInt();
		}
	};

	public static final BufferSerializer<Integer> VARINT_ZIGZAG_SERIALIZER = new BufferSerializer<Integer>() {
		@Override
		public void serialize(ByteBuf output, Integer item) {
			output.writeVarInt((item << 1) ^ (item >> 31));
		}

		@Override
		public Integer deserialize(ByteBuf input) {
			int n = input.readVarInt();
			return (n >>> 1) ^ -(n & 1);
		}
	};

	public static final BufferSerializer<Long> LONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public void serialize(ByteBuf output, Long item) {
			output.writeLong(item);
		}

		@Override
		public Long deserialize(ByteBuf input) {
			return input.readLong();
		}
	};

	public static final BufferSerializer<Long> VARLONG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public void serialize(ByteBuf output, Long item) {
			output.writeVarLong(item);
		}

		@Override
		public Long deserialize(ByteBuf input) {
			return input.readVarLong();
		}
	};

	public static final BufferSerializer<Long> VARLONG_ZIGZAG_SERIALIZER = new BufferSerializer<Long>() {
		@Override
		public void serialize(ByteBuf output, Long item) {
			output.writeVarLong((item << 1) ^ (item >> 63));
		}

		@Override
		public Long deserialize(ByteBuf input) {
			long n = input.readVarLong();
			return (n >>> 1) ^ -(n & 1);
		}
	};

	public static final BufferSerializer<Float> FLOAT_SERIALIZER = new BufferSerializer<Float>() {
		@Override
		public void serialize(ByteBuf output, Float item) {
			output.writeFloat(item);
		}

		@Override
		public Float deserialize(ByteBuf input) {
			return input.readFloat();
		}
	};

	public static final BufferSerializer<Double> DOUBLE_SERIALIZER = new BufferSerializer<Double>() {
		@Override
		public void serialize(ByteBuf output, Double item) {
			output.writeDouble(item);
		}

		@Override
		public Double deserialize(ByteBuf input) {
			return input.readDouble();
		}
	};

	public static final BufferSerializer<Character> CHAR_SERIALIZER = new BufferSerializer<Character>() {
		@Override
		public void serialize(ByteBuf output, Character item) {
			output.writeChar(item);
		}

		@Override
		public Character deserialize(ByteBuf input) {
			return input.readChar();
		}
	};

	public static final BufferSerializer<String> JAVA_UTF8_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public void serialize(ByteBuf output, String item) {
			output.writeJavaUTF8(item);
		}

		@Override
		public String deserialize(ByteBuf input) {
			return input.readJavaUTF8();
		}
	};

	public static final BufferSerializer<String> UTF16_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public void serialize(ByteBuf output, String item) {
			output.writeUTF16(item);
		}

		@Override
		public String deserialize(ByteBuf input) {
			return input.readUTF16();
		}
	};

	public static final BufferSerializer<Boolean> BOOLEAN_SERIALIZER = new BufferSerializer<Boolean>() {
		@Override
		public void serialize(ByteBuf output, Boolean item) {
			output.writeBoolean(item);
		}

		@Override
		public Boolean deserialize(ByteBuf input) {
			return input.readBoolean();
		}
	};

	public static final BufferSerializer<String> ISO_8859_1_SERIALIZER = new BufferSerializer<String>() {
		@Override
		public void serialize(ByteBuf output, String item) {
			output.writeIso88591(item);
		}

		@Override
		public String deserialize(ByteBuf input) {
			return input.readIso88591();
		}
	};

	public static BufferSerializer<Integer> varIntSerializer(boolean optimizePositive) {
		return optimizePositive ? VARINT_SERIALIZER : VARINT_ZIGZAG_SERIALIZER;
	}

	public static BufferSerializer<Long> varLongSerializer(boolean optimizePositive) {
		return optimizePositive ? VARLONG_SERIALIZER : VARLONG_ZIGZAG_SERIALIZER;
	}

	private static <E, C extends Collection<E>> BufferSerializer<C> ofCollection(BufferSerializer<E> element, Supplier<C> constructor) {
		return new BufferSerializer<C>() {
			@Override
			public void serialize(ByteBuf output, C item) {
				output.writeInt(item.size());
				item.forEach(elem -> element.serialize(output, elem));
			}

			@Override
			public C deserialize(ByteBuf input) {
				C coll = constructor.get();
				int size = input.readInt();
				for (int i = 0; i < size; i++) {
					coll.add(element.deserialize(input));
				}
				return coll;
			}
		};
	}

	public static <E> BufferSerializer<List<E>> ofList(BufferSerializer<E> element) {
		return ofCollection(element, ArrayList::new);
	}

	@SuppressWarnings("unchecked")
	public static <E> BufferSerializer<E[]> ofArray(BufferSerializer<E> element) {
		return ofList(element).transform(Arrays::asList, value -> (E[]) value.toArray());
	}

	public static <E> BufferSerializer<Set<E>> ofSet(BufferSerializer<E> element) {
		return ofCollection(element, LinkedHashSet::new);
	}

	public static <K, V> BufferSerializer<Map<K, V>> ofMap(BufferSerializer<K> key, BufferSerializer<V> value) {
		return new BufferSerializer<Map<K, V>>() {
			@Override
			public void serialize(ByteBuf output, Map<K, V> item) {
				output.writeInt(item.size());
				item.forEach((k, v) -> {
					key.serialize(output, k);
					value.serialize(output, v);
				});
			}

			@Override
			public Map<K, V> deserialize(ByteBuf input) {
				Map<K, V> map = new LinkedHashMap<>();
				int size = input.readInt();
				for (int i = 0; i < size; i++) {
					map.put(key.deserialize(input), value.deserialize(input));
				}
				return map;
			}
		};
	}
}
