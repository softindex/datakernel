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

package io.datakernel.csp.binary;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.api.ParserFunction;
import io.datakernel.common.exception.parse.InvalidSizeException;
import io.datakernel.common.exception.parse.ParseException;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.csp.binary.Utils.parseUntilTerminatorByte;
import static java.lang.Math.min;

@FunctionalInterface
public interface ByteBufsDecoder<T> {
    ParseException SIZE_EXCEEDS_MAX_SIZE = new InvalidSizeException(ByteBufsDecoder.class, "Size exceeds max size");
    ParseException NEGATIVE_SIZE = new InvalidSizeException(ByteBufsDecoder.class, "Invalid size of bytes to be read, should be greater than 0");

    @Nullable
    T tryDecode(ByteBufQueue bufs) throws ParseException;

    default <V> ByteBufsDecoder<V> andThen(ParserFunction<? super T, ? extends V> after) {
        return bufs -> {
            T maybeResult = tryDecode(bufs);
            if (maybeResult == null) return null;
            return after.parse(maybeResult);
        };
    }

    static ByteBufsDecoder<byte[]> assertBytes(byte[] data) {
        return bufs -> {
            if (!bufs.hasRemainingBytes(data.length)) return null;
            for (int i = 0; i < data.length; i++) {
                if (data[i] != bufs.peekByte(i)) {
                    throw new ParseException(ByteBufsDecoder.class, "Array of bytes differs at index " + i +
                            "[Expected: " + data[i] + ", actual: " + bufs.peekByte(i) + ']');
                }
            }
            bufs.skip(data.length);
            return data;
        };
    }

    static ByteBufsDecoder<ByteBuf> ofFixedSize(int length) {
        return bufs -> {
            if (!bufs.hasRemainingBytes(length)) return null;
            return bufs.takeExactSize(length);
        };
    }

    static ByteBufsDecoder<ByteBuf> ofNullTerminatedBytes() {
        return ofNullTerminatedBytes(Integer.MAX_VALUE);
    }

    static ByteBufsDecoder<ByteBuf> ofNullTerminatedBytes(int maxSize) {
        return parseUntilTerminatorByte((byte) 0, maxSize);
    }

    static ByteBufsDecoder<ByteBuf> ofCrTerminatedBytes() {
        return ofCrTerminatedBytes(Integer.MAX_VALUE);
    }

    static ByteBufsDecoder<ByteBuf> ofCrTerminatedBytes(int maxSize) {
        return parseUntilTerminatorByte(CR, maxSize);
    }

    static ByteBufsDecoder<ByteBuf> ofCrlfTerminatedBytes() {
        return ofCrlfTerminatedBytes(Integer.MAX_VALUE);
    }

    static ByteBufsDecoder<ByteBuf> ofCrlfTerminatedBytes(int maxSize) {
        return bufs -> {
            for (int i = 0; i < min(bufs.remainingBytes() - 1, maxSize); i++) {
                if (bufs.peekByte(i) == CR && bufs.peekByte(i + 1) == LF) {
                    ByteBuf buf = bufs.takeExactSize(i);
                    bufs.skip(2);
                    return buf;
                }
            }
            if (bufs.remainingBytes() >= maxSize) {
                throw new ParseException(ByteBufsDecoder.class, "No CRLF is found in " + maxSize + " bytes");
            }
            return null;
        };
    }

    static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes() {
        return ofIntSizePrefixedBytes(Integer.MAX_VALUE);
    }

    static ByteBufsDecoder<ByteBuf> ofIntSizePrefixedBytes(int maxSize) {
        return bufs -> {
            if (!bufs.hasRemainingBytes(4)) return null;
            int size = (bufs.peekByte(0) & 0xFF) << 24
                    | (bufs.peekByte(1) & 0xFF) << 16
                    | (bufs.peekByte(2) & 0xFF) << 8
                    | (bufs.peekByte(3) & 0xFF);
            if (size < 0 || size > maxSize) {
                throw new InvalidSizeException(ByteBufsDecoder.class,
                        "Size is either less than 0 or greater than maxSize. Parsed size: " + size);
            }
            if (!bufs.hasRemainingBytes(4 + size)) return null;
            bufs.skip(4);
            return bufs.takeExactSize(size);
        };
    }

    static ByteBufsDecoder<ByteBuf> ofShortSizePrefixedBytes() {
        return ofShortSizePrefixedBytes(Integer.MAX_VALUE);
    }

    static ByteBufsDecoder<ByteBuf> ofShortSizePrefixedBytes(int maxSize) {
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

    static ByteBufsDecoder<ByteBuf> ofByteSizePrefixedBytes() {
        return ofByteSizePrefixedBytes(Integer.MAX_VALUE);
    }

    static ByteBufsDecoder<ByteBuf> ofByteSizePrefixedBytes(int maxSize) {
        return bufs -> {
            if (!bufs.hasRemaining()) return null;
            int size = bufs.peekByte(0) & 0xFF;
            if (size > maxSize) throw SIZE_EXCEEDS_MAX_SIZE;
            if (!bufs.hasRemainingBytes(1 + size)) return null;
            bufs.skip(1);
            return bufs.takeExactSize(size);
        };
    }

    static ByteBufsDecoder<ByteBuf> ofVarIntSizePrefixedBytes() {
        return ofVarIntSizePrefixedBytes(Integer.MAX_VALUE);
    }

    static ByteBufsDecoder<ByteBuf> ofVarIntSizePrefixedBytes(int maxSize) {
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
                                throw new ParseException(ByteBufsDecoder.class, "Varint is too long for 32-bit integer");
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

}
