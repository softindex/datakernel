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

package io.datakernel.serializer.util;

import io.datakernel.serializer.AbstractBinarySerializer;
import io.datakernel.serializer.BinarySerializer;

import java.util.*;
import java.util.function.Supplier;

public final class BinarySerializers {
	private BinarySerializers() {}

	public static final BinarySerializer<Byte> BYTE_SERIALIZER = new AbstractBinarySerializer<Byte>() {
		@Override
		public void encode(BinaryOutput out, Byte item) throws ArrayIndexOutOfBoundsException {
			out.writeByte(item);
		}

		@Override
		public Byte decode(BinaryInput in) {
			return in.readByte();
		}
	};

	public static final BinarySerializer<Integer> INT_SERIALIZER = new AbstractBinarySerializer<Integer>() {
		@Override
		public void encode(BinaryOutput out, Integer item) throws ArrayIndexOutOfBoundsException {
			out.writeInt(item);
		}

		@Override
		public Integer decode(BinaryInput in) {
			return in.readInt();
		}
	};

	public static final BinarySerializer<Long> LONG_SERIALIZER = new AbstractBinarySerializer<Long>() {
		@Override
		public void encode(BinaryOutput out, Long item) throws ArrayIndexOutOfBoundsException {
			out.writeLong(item);
		}

		@Override
		public Long decode(BinaryInput in) {
			return in.readLong();
		}
	};

	public static final BinarySerializer<Float> FLOAT_SERIALIZER = new AbstractBinarySerializer<Float>() {
		@Override
		public void encode(BinaryOutput out, Float item) throws ArrayIndexOutOfBoundsException {
			out.writeFloat(item);
		}

		@Override
		public Float decode(BinaryInput in) {
			return in.readFloat();
		}
	};

	public static final BinarySerializer<Double> DOUBLE_SERIALIZER = new AbstractBinarySerializer<Double>() {
		@Override
		public void encode(BinaryOutput out, Double item) throws ArrayIndexOutOfBoundsException {
			out.writeDouble(item);
		}

		@Override
		public Double decode(BinaryInput in) {
			return in.readDouble();
		}
	};

	public static final BinarySerializer<String> UTF8_SERIALIZER = new AbstractBinarySerializer<String>() {
		@Override
		public void encode(BinaryOutput out, String item) throws ArrayIndexOutOfBoundsException {
			out.writeUTF8(item);
		}

		@Override
		public String decode(BinaryInput in) {
			return in.readUTF8();
		}
	};

	public static final BinarySerializer<String> ISO_88591_SERIALIZER = new AbstractBinarySerializer<String>() {
		@Override
		public void encode(BinaryOutput out, String item) throws ArrayIndexOutOfBoundsException {
			out.writeIso88591(item);
		}

		@Override
		public String decode(BinaryInput in) {
			return in.readIso88591();
		}
	};

	public static final BinarySerializer<String> UTF8_MB3_SERIALIZER = new AbstractBinarySerializer<String>() {
		@Override
		public void encode(BinaryOutput out, String item) throws ArrayIndexOutOfBoundsException {
			out.writeUTF8mb3(item);
		}

		@Override
		public String decode(BinaryInput in) {
			return in.readUTF8mb3();
		}
	};

	public static final BinarySerializer<byte[]> BYTES_SERIALIZER = new AbstractBinarySerializer<byte[]>() {
		@Override
		public void encode(BinaryOutput out, byte[] item) throws ArrayIndexOutOfBoundsException {
			out.writeVarInt(item.length);
			out.write(item);
		}

		@Override
		public byte[] decode(BinaryInput in) {
			int size = in.readVarInt();
			byte[] bytes = new byte[size];
			in.read(bytes);
			return bytes;
		}
	};

	public static <T> BinarySerializer<Optional<T>> ofOptional(BinarySerializer<T> codec) {
		return new AbstractBinarySerializer<Optional<T>>() {
			final BinarySerializer<T> nullable = ofNullable(codec);

			@Override
			public void encode(BinaryOutput out, Optional<T> item) {
				nullable.encode(out, item.orElse(null));
			}

			@Override
			public Optional<T> decode(BinaryInput in) {
				return Optional.ofNullable(nullable.decode(in));
			}
		};
	}

	@SuppressWarnings("unchecked")
	public static <T> BinarySerializer<T> ofNullable(BinarySerializer<T> codec) {
		if (codec == UTF8_SERIALIZER) {
			return (BinarySerializer<T>) new AbstractBinarySerializer<String>() {
				@Override
				public void encode(BinaryOutput out, String item) {
					out.writeUTF8Nullable(item);
				}

				@Override
				public String decode(BinaryInput in) {
					return in.readUTF8Nullable();
				}
			};
		}
		if (codec == ISO_88591_SERIALIZER) {
			return (BinarySerializer<T>) new AbstractBinarySerializer<String>() {
				@Override
				public void encode(BinaryOutput out, String item) {
					out.writeIso88591Nullable(item);
				}

				@Override
				public String decode(BinaryInput in) {
					return in.readIso88591Nullable();
				}
			};
		}
		if (codec == UTF8_MB3_SERIALIZER) {
			return (BinarySerializer<T>) new AbstractBinarySerializer<String>() {
				@Override
				public void encode(BinaryOutput out, String item) {
					out.writeUTF8mb3Nullable(item);
				}

				@Override
				public String decode(BinaryInput in) {
					return in.readUTF8mb3Nullable();
				}
			};
		}
		return new AbstractBinarySerializer<T>() {
			@Override
			public void encode(BinaryOutput out, T item) {
				if (item != null) {
					out.writeBoolean(true);
					codec.encode(out, item);
				} else {
					out.writeBoolean(false);
				}
			}

			@Override
			public T decode(BinaryInput in) {
				if (in.readBoolean()) {
					return codec.decode(in);
				} else {
					return null;
				}
			}
		};
	}

	private static <E, C extends Collection<E>> BinarySerializer<C> ofCollection(BinarySerializer<E> element, Supplier<C> constructor) {
		return new AbstractBinarySerializer<C>() {
			@Override
			public void encode(BinaryOutput out, C item) {
				out.writeVarInt(item.size());
				for (E v : item) {
					element.encode(out, v);
				}
			}

			@Override
			public C decode(BinaryInput in) {
				C collection = constructor.get();
				int size = in.readVarInt();
				for (int i = 0; i < size; i++) {
					collection.add(element.decode(in));
				}
				return collection;
			}
		};
	}

	public static <E> BinarySerializer<List<E>> ofList(BinarySerializer<E> element) {
		return ofCollection(element, ArrayList::new);
	}

	public static <E> BinarySerializer<Set<E>> ofSet(BinarySerializer<E> element) {
		return ofCollection(element, LinkedHashSet::new);
	}

	public static <K, V> BinarySerializer<Map<K, V>> ofMap(BinarySerializer<K> key, BinarySerializer<V> value) {
		return new AbstractBinarySerializer<Map<K, V>>() {
			@Override
			public void encode(BinaryOutput out, Map<K, V> item) {
				out.writeVarInt(item.size());
				for (Map.Entry<K, V> entry : item.entrySet()) {
					key.encode(out, entry.getKey());
					value.encode(out, entry.getValue());
				}
			}

			@Override
			public Map<K, V> decode(BinaryInput in) {
				Map<K, V> map = new LinkedHashMap<>();
				int size = in.readVarInt();
				for (int i = 0; i < size; i++) {
					map.put(key.decode(in), value.decode(in));
				}
				return map;
			}
		};
	}
}
