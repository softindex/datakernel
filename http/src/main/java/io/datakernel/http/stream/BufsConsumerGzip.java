package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;

import java.util.zip.CRC32;
import java.util.zip.Deflater;

public class BufsConsumerGzip implements BufsConsumer {
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
	private static final int GZIP_FOOTER_SIZE = 8;

	private final BufsConsumer next;

	private final Deflater deflater;
	private final int size;
	private final CRC32 crc32 = new CRC32();
	private final ByteBufQueue outputBufs = new ByteBufQueue();

	public BufsConsumerGzip(BufsConsumer next, Deflater deflater, int size) {
		this.next = next;
		this.deflater = deflater;
		this.size = size;
	}

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		outputBufs.add(ByteBuf.wrapForReading(GZIP_HEADER));

		while (!inputBufs.isEmpty()) {
			ByteBuf src = inputBufs.take();
			crc32.update(src.array(), src.readPosition(), src.readRemaining());
			deflater.setInput(src.array(), src.readPosition(), src.readRemaining());
			src.recycle();
			while (!deflater.needsInput()) {
				deflate(outputBufs);
			}

			deflater.setInput(src.array(), src.readPosition(), src.readRemaining());
		}

		if (endOfStream) {
			deflater.finish();
			while (!deflater.finished()) {
				deflate(outputBufs);
			}
			ByteBuf footer = ByteBufPool.allocate(GZIP_FOOTER_SIZE);
			footer.writeInt(Integer.reverseBytes((int) crc32.getValue()));
			footer.writeInt(Integer.reverseBytes(deflater.getTotalIn()));
			outputBufs.add(footer);
		}
		return null;
	}

	@Override
	public void closeWithError(Throwable e) {
		deflater.end();
		outputBufs.recycle();
		next.closeWithError(e);
	}

	private void deflate(ByteBufQueue outputQueue) {
		ByteBuf out = ByteBufPool.allocate(size);
		int len = deflater.deflate(out.array(), out.writePosition(), out.writeRemaining());
		if (len > 0) {
			out.writePosition(len);
			outputQueue.add(out);
		}
	}

}
