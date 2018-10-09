package io.datakernel.serial;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;

class Utils {
	private Utils() {}

	static ByteBufsParser<ByteBuf> parseUntilTerminatorByte(byte terminator, int maxSize) {
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
