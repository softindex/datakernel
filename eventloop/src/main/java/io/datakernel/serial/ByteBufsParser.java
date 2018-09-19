package io.datakernel.serial;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.functional.Try;

import java.util.function.Function;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static java.lang.Math.min;

public interface ByteBufsParser<T> {
	@Nullable
	T tryParse(ByteBufQueue bufs) throws ParseException;

	default Stage<T> parse(ByteBufsSupplier supplier) {
		return supplier.parse(this);
	}

	default <V> ByteBufsParser<V> andThen(Function<? super T, ? extends V> after) {
		return bufs -> {
			T maybeResult = tryParse(bufs);
			if (maybeResult == null) return null;
			return after.apply(maybeResult);
		};
	}

	static <T> ByteBufsParser of(Function<ByteBufQueue, Try<T>> fn) {
		return bufs -> {
			Try<T> maybeResult = fn.apply(bufs);
			if (maybeResult == null) return null;
			if (maybeResult.isSuccess()) return maybeResult.getResult();
			Throwable e = maybeResult.getException();
			if (e instanceof ParseException) throw (ParseException) e;
			if (e instanceof RuntimeException) throw (RuntimeException) e;
			if (e instanceof Exception) throw new ParseException(e);
			throw new RuntimeException(e);
		};
	}

	static ByteBufsParser<byte[]> assertBytes(byte[] data) {
		return bufs -> {
			if (bufs.remainingBytes() < data.length) return null;
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
			if (bufs.remainingBytes() < length) return null;
			return bufs.takeExactSize(length);
		};
	}

	static ByteBufsParser<ByteBuf> ofNullTerminatedBytes() {
		return ofNullTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofNullTerminatedBytes(int maxSize) {
		return parseUntillTerminatorByte((byte) 0, maxSize);
	}

	static ByteBufsParser<ByteBuf> ofCrTerminatedBytes() {
		return ofCrTerminatedBytes(Integer.MAX_VALUE);
	}

	static ByteBufsParser<ByteBuf> ofCrTerminatedBytes(int maxSize) {
		return parseUntillTerminatorByte(CR, maxSize);
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
			if (bufs.remainingBytes() < 4) return null;
			int size = (bufs.peekByte(0) << 24
					| (bufs.peekByte(1) & 0xFF) << 16
					| (bufs.peekByte(2) & 0xFF) << 8
					| (bufs.peekByte(3) & 0xFF));
			if (size > maxSize) throw new ParseException();
			if (bufs.remainingBytes() < 4 + size) return null;
			bufs.skip(4);
			return bufs.takeExactSize(size);
		};
	}

	static ByteBufsParser<ByteBuf> parseUntillTerminatorByte(byte terminator, int maxSize) {
		return bufs -> {
			for (int i = 0; i < Math.min(bufs.remainingBytes(), maxSize); i++) {
				if (bufs.peekByte(i) == terminator) {
					ByteBuf buf = bufs.takeExactSize(i);
					bufs.skip(1);
					return buf;
				}
			}
			if (bufs.remainingBytes() >= maxSize) throw new ParseException();
			return null;
		};
	}
}
