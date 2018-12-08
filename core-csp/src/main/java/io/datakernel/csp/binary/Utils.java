package io.datakernel.csp.binary;

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
			if (bufs.remainingBytes() >= maxSize) {
				throw new ParseException(ByteBufsParser.class, "No terminator byte is found in " + maxSize + " bytes");
			}
			return null;
		};
	}
}
