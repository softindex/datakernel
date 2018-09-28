package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.serial.Utils.parseUntilTerminatorByte;
import static java.lang.Math.min;

public interface ByteBufsParser<T> {
	@Nullable
	T tryParse(ByteBufQueue bufs) throws ParseException;

	default Stage<T> parse(ByteBufsSupplier supplier) {
		return supplier.parse(this);
	}

	interface ParseFunction<T, R> {
		R apply(T t) throws ParseException;
	}

	default <V> ByteBufsParser<V> andThen(ParseFunction<? super T, ? extends V> after) {
		return bufs -> {
			T maybeResult = tryParse(bufs);
			if (maybeResult == null) return null;
			return after.apply(maybeResult);
		};
	}

	static ByteBufsParser<byte[]> assertBytes(byte[] data) {
		return bufs -> {
			if (!bufs.hasRemainingBytes(data.length)) return null;
			for (int i = 0; i < data.length; i++) {
				if (data[i] != bufs.peekByte(i)) {
					throw new ParseException();
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
			if (bufs.remainingBytes() >= maxSize) throw new ParseException();
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
			if (size < 0 || size > maxSize) throw new ParseException();
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
			if (size < 0 || size > maxSize) throw new ParseException();
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
			if (size > maxSize) throw new ParseException();
			if (!bufs.hasRemainingBytes(1 + size)) return null;
			bufs.skip(1);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsParser<ByteBuf> ofVarIntSizePrefixedBytes() {
		return ofVarIntSizePrefixedBytes(Integer.MAX_VALUE);
	}

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
							} else
								throw new ParseException();
						}
					}
				}
			}
			if (size < 0 || size > maxSize) throw new ParseException();
			bufs.skip(prefixSize);
			return bufs.takeExactSize(size);
		};
	}

}
