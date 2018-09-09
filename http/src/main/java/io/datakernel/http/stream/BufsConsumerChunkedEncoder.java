package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;

public final class BufsConsumerChunkedEncoder implements BufsConsumer {
	private static final byte[] LAST_CHUNK = {48, 13, 10, 13, 10};

	private final BufsConsumer next;
	private final ByteBufQueue outputBufs = new ByteBufQueue();

	public BufsConsumerChunkedEncoder(BufsConsumer next) {
		this.next = next;
	}

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		while (!inputBufs.isEmpty()) {
			ByteBuf buf = inputBufs.take();
			ByteBuf chunkBuf = toChunkBuf(buf);
			outputBufs.add(chunkBuf);
		}
		if (endOfStream) {
			writeLastChunk();
		}

		return next.push(outputBufs, endOfStream)
				.whenException(this::closeWithError);
	}

	private void writeLastChunk() {
		ByteBuf endOfStreamBuf = ByteBufPool.allocate(5);
		// writing "0\r\n\r\n" aka last-chunk
		endOfStreamBuf.write(LAST_CHUNK);
		endOfStreamBuf.writePosition(5);
		outputBufs.add(endOfStreamBuf);
	}

	private static ByteBuf toChunkBuf(ByteBuf buf) {
		int bufSize = buf.readRemaining();
		char[] hexRepr = Integer.toHexString(bufSize).toCharArray();
		int hexLen = hexRepr.length;
		ByteBuf chunkBuf = ByteBufPool.allocate(hexLen + 2 + bufSize + 2);
		byte[] chunkArray = chunkBuf.array();
		for (int i = 0; i < hexLen; i++) {
			chunkArray[i] = (byte) hexRepr[i];
		}
		chunkArray[hexLen] = CR;
		chunkArray[hexLen + 1] = LF;
		chunkBuf.writePosition(hexLen + 2);
		chunkBuf.put(buf);
		buf.recycle();
		chunkBuf.writeByte(CR);
		chunkBuf.writeByte(LF);
		return chunkBuf;
	}

	@Override
	public void closeWithError(Throwable e) {
		outputBufs.recycle();
		next.closeWithError(e);
	}
}
