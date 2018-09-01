package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.util.Preconditions.check;

public final class BufsConsumerChunkedDecoder implements BufsConsumer {
	public static final ParseException MALFORMED_CHUNK = new ParseException("Malformed chunk");
	public static final int MAX_CHUNK_HEADER_CHARS = 16;

	private final BufsConsumer next;

	private final ByteBufQueue outputBufs = new ByteBufQueue();
	private static final byte NOTHING = 0;
	private static final byte CHUNK_LENGTH = 1;
	private static final byte CHUNK = 2;
	private byte reading = CHUNK_LENGTH;
	private int chunkSize;

	public BufsConsumerChunkedDecoder(BufsConsumer next) {
		this.next = next;
	}

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		while (!inputBufs.isEmpty()) {
			if (reading == CHUNK_LENGTH) {
				byte c = inputBufs.peekByte();
				if (c == SP) {
					inputBufs.getByte();
				} else if (c >= '0' && c <= '9') {
					chunkSize = (chunkSize << 4) + (c - '0');
					inputBufs.getByte();
				} else if (c >= 'a' && c <= 'f') {
					chunkSize = (chunkSize << 4) + (c - 'a' + 10);
					inputBufs.getByte();
				} else if (c >= 'A' && c <= 'F') {
					chunkSize = (chunkSize << 4) + (c - 'A' + 10);
					inputBufs.getByte();
				} else {
					if (chunkSize != 0) {
						if (!inputBufs.hasRemainingBytes(2)) {
							break;
						}
						byte c1 = inputBufs.getByte();
						byte c2 = inputBufs.getByte();
						check(c1 == CR && c2 == LF, MALFORMED_CHUNK);
						reading = CHUNK;
					} else {
						if (!inputBufs.hasRemainingBytes(4)) {
							break;
						}
						byte c1 = inputBufs.getByte();
						byte c2 = inputBufs.getByte();
						byte c3 = inputBufs.getByte();
						byte c4 = inputBufs.getByte();
						check(c1 == CR && c2 == LF && c3 == CR && c4 == LF, MALFORMED_CHUNK);
						return next.push(outputBufs, true);
					}
				}
			}
			if (reading == CHUNK) {
				chunkSize -= inputBufs.drainTo(outputBufs, chunkSize);
				if (chunkSize == 0) {
					if (!inputBufs.hasRemainingBytes(2)) {
						break;
					}
					if (inputBufs.peekByte() == ';') {
						int remainingBytes = inputBufs.remainingBytes();
						for (int i = 0; i < remainingBytes - 1; i++) {
							if (inputBufs.peekByte(i) == CR && inputBufs.peekByte(i + 1) == LF) {
								inputBufs.skip(i);
							}
						}
					} else {
						byte c1 = inputBufs.getByte();
						byte c2 = inputBufs.getByte();
						check(c1 == CR && c2 == LF, MALFORMED_CHUNK);
						reading = CHUNK_LENGTH;
					}
				}
			}
		}

		return next.push(inputBufs, false);
	}

	@Override
	public void closeWithError(Throwable e) {
		outputBufs.recycle();
		next.closeWithError(e);
	}

}
