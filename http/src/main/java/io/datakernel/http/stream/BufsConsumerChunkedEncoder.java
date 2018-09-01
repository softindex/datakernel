package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static java.lang.Integer.numberOfLeadingZeros;

public final class BufsConsumerChunkedEncoder implements BufsConsumer {
	private final BufsConsumer next;
	private final ByteBufQueue outputBufs = new ByteBufQueue();

	public BufsConsumerChunkedEncoder(BufsConsumer next) {
		this.next = next;
	}

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		while (!inputBufs.isEmpty()) {
			ByteBuf buf = inputBufs.take();
			ByteBuf headerBuf = headerBuf(buf);
			outputBufs.add(headerBuf);
			outputBufs.add(buf);
		}
		if (endOfStream) {
			ByteBuf endOfStreamBuf = ByteBufPool.allocate(4);
			byte[] array = endOfStreamBuf.array();
			array[0] = CR;
			array[1] = LF;
			array[2] = CR;
			array[3] = LF;
			endOfStreamBuf.writePosition(4);
			outputBufs.add(endOfStreamBuf);
		}
		return null;
	}

	@Override
	public void closeWithError(Throwable e) {
		outputBufs.recycle();
		next.closeWithError(e);
	}

	static ByteBuf headerBuf(ByteBuf buf) {
		int bufSize = buf.readRemaining();
		int bufSizeBits = 32 - numberOfLeadingZeros(bufSize - 1);
		int bufSizeBytes = (bufSizeBits + 1) >>> 3;

		ByteBuf headerBuf = ByteBufPool.allocate(16);
		byte[] headersArray = headerBuf.array();

		for (int i = 0; i < bufSizeBytes * 2; i++) {
			byte b = (byte) ((bufSize >>> (bufSizeBytes - i)) * 0xF);
			headersArray[i] = (byte) (b <= 9 ? '0' + b : 'A' + b - 10);
		}
		headersArray[bufSizeBytes] = CR;
		headersArray[bufSizeBytes + 1] = LF;
		headerBuf.writePosition(bufSizeBytes + 2);
		return headerBuf;
	}

}
