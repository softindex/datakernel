/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.InvalidSizeException;
import io.datakernel.exception.ParseException;
import io.datakernel.util.ParserFunction;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.serial.Utils.parseUntilTerminatorByte;
import static java.lang.Math.min;

public interface ByteBufsParser<T> {
	ParseException SIZE_EXCEEDS_MAX_SIZE = new InvalidSizeException(ByteBufsParser.class, "Size exceeds max size");
	ParseException NEGATIVE_SIZE = new InvalidSizeException(ByteBufsParser.class, "Invalid size of bytes to be read, should be greater than 0");

	@Nullable
	T tryParse(ByteBufQueue bufs) throws ParseException;

	default Promise<T> parse(ByteBufsSupplier supplier) {
		return supplier.parse(this);
	}

	default <V> ByteBufsParser<V> andThen(ParserFunction<? super T, ? extends V> after) {
		return bufs -> {
			T maybeResult = tryParse(bufs);
			if (maybeResult == null) return null;
			return after.parse(maybeResult);
		};
	}

	static ByteBufsParser<byte[]> assertBytes(byte[] data) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(data.length)) return null;
			for (int i = 0; i < data.length; i++) {
				if (data[i] != bufs.peekByte(i)) {
					throw new ParseException(ByteBufsParser.class, "Array of bytes differs at index " + i +
							"[Expected: " + data[i] + ", actual: " + bufs.peekByte(i) + ']');
				}
			}
			bufs.skip(data.length);
			return data;
		};
	}

	static ByteBufsParser<ByteBuf> ofFixedSize(int length) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(length)) return null;
			return bufs.takeExactSize(length);
		};
	}

	static ByteBufsParser<ByteBuf> ofNullTerminatedBytes() {
		return ofNullTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofNullTerminatedBytes(int maxSize) {
		return parseUntilTerminatorByte((byte) 0, maxSize);
	}

	static ByteBufsParser<ByteBuf> ofCrTerminatedBytes() {
		return ofCrTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofCrTerminatedBytes(int maxSize) {
		return parseUntilTerminatorByte(CR, maxSize);
	}

	static ByteBufsParser<ByteBuf> ofCrlfTerminatedBytes() {
		return ofCrlfTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofCrlfTerminatedBytes(int maxSize) {
		return bufs -> {
			for (int i = 0; i < min(bufs.remainingBytes() - 1, maxSize); i++) {
				if (bufs.peekByte(i) == CR && bufs.peekByte(i + 1) == LF) {
					ByteBuf buf = bufs.takeExactSize(i);
					bufs.skip(2);
					return buf;
				}
			}
			if (bufs.remainingBytes() >= maxSize) {
				throw new ParseException(ByteBufsParser.class, "No CRLF is found in " + maxSize + " bytes");
			}
			return null;
		};
	}

	static ByteBufsParser<ByteBuf> ofIntSizePrefixedBytes() {
		return ofIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofIntSizePrefixedBytes(int maxSize) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(4)) return null;
			int size = (bufs.peekByte(0) & 0xFF) << 24
					| (bufs.peekByte(1) & 0xFF) << 16
					| (bufs.peekByte(2) & 0xFF) << 8
					| (bufs.peekByte(3) & 0xFF);
			if (size < 0 || size > maxSize) {
				throw new InvalidSizeException(ByteBufsParser.class,
						"Size is either less than 0 or greater than maxSize. Parsed size: " + size);
			}
			if (!bufs.hasRemainingBytes(4 + size)) return null;
			bufs.skip(4);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsParser<ByteBuf> ofShortSizePrefixedBytes() {
		return ofShortSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofShortSizePrefixedBytes(int maxSize) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(2)) return null;
			int size = (bufs.peekByte(0) & 0xFF) << 8
					| (bufs.peekByte(1) & 0xFF);
			if (size > maxSize) throw SIZE_EXCEEDS_MAX_SIZE;
			if (!bufs.hasRemainingBytes(2 + size)) return null;
			bufs.skip(2);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsParser<ByteBuf> ofByteSizePrefixedBytes() {
		return ofByteSizePrefixedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofByteSizePrefixedBytes(int maxSize) {
		return bufs -> {
			if (!bufs.hasRemaining()) return null;
			int size = bufs.peekByte(0) & 0xFF;
			if (size > maxSize) throw SIZE_EXCEEDS_MAX_SIZE;
			if (!bufs.hasRemainingBytes(1 + size)) return null;
			bufs.skip(1);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsParser<ByteBuf> ofVarIntSizePrefixedBytes() {
		return ofVarIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

	// region creators
	static ByteBufsParser<ByteBuf> ofVarIntSizePrefixedBytes(int maxSize) {
		return bufs -> {
			int size;
			int prefixSize;
			if (!bufs.hasRemainingBytes(1)) return null;
			byte b = bufs.peekByte(0);
			if (b >= 0) {
				size = b;
				prefixSize = 1;
			} else {
				if (!bufs.hasRemainingBytes(2)) return null;
				size = b & 0x7f;
				if ((b = bufs.peekByte(1)) >= 0) {
					size |= b << 7;
					prefixSize = 2;
				} else {
					if (!bufs.hasRemainingBytes(3)) return null;
					size |= (b & 0x7f) << 7;
					if ((b = bufs.peekByte(2)) >= 0) {
						size |= b << 14;
						prefixSize = 3;
					} else {
						if (!bufs.hasRemainingBytes(4)) return null;
						size |= (b & 0x7f) << 14;
						if ((b = bufs.peekByte(3)) >= 0) {
							size |= b << 21;
							prefixSize = 4;
						} else {
							if (!bufs.hasRemainingBytes(5)) return null;
							size |= (b & 0x7f) << 21;
							if ((b = bufs.peekByte(4)) >= 0) {
								size |= b << 28;
								prefixSize = 5;
							} else {
								throw new ParseException(ByteBufsParser.class, "Varint is too long for 32-bit integer");
							}
						}
					}
				}
			}
			if (size < 0) throw NEGATIVE_SIZE;
			if (size > maxSize) throw SIZE_EXCEEDS_MAX_SIZE;
			if (!bufs.hasRemainingBytes(prefixSize + size)) return null;
			bufs.skip(prefixSize);
			return bufs.takeExactSize(size);
		};
	}
	// endregion

}
