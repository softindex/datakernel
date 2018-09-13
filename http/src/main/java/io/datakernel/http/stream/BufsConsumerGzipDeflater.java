package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;

import java.util.zip.CRC32;
import java.util.zip.Deflater;

public final class BufsConsumerGzipDeflater implements BufsConsumer {
	public static final int DEFAULT_MAX_BUF_SIZE = 512;
	// rfc 1952 section 2.3.1
	private static final byte[] GZIP_HEADER = {(byte) 0x1f, (byte) 0x8b, Deflater.DEFLATED, 0, 0, 0, 0, 0, 0, 0};
	private static final int GZIP_FOOTER_SIZE = 8;
	private static final byte HEADER = 1;
	private static final byte BODY = 2;
	private static final byte FOOTER = 3;

	private final BufsConsumer next;
	private final Deflater deflater;
	private final int maxBufSize;
	private final CRC32 crc32 = new CRC32();
	private final ByteBufQueue outputBufs = new ByteBufQueue();

	private byte writing = HEADER;

	// region creators
	public BufsConsumerGzipDeflater(BufsConsumer next, Deflater deflater, int maxBufSize) {
		this.next = next;
		this.deflater = deflater;
		this.maxBufSize = maxBufSize;
	}

	public BufsConsumerGzipDeflater(BufsConsumer next) {
		this.next = next;
		this.deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
		this.maxBufSize = DEFAULT_MAX_BUF_SIZE;
	}
	// endregion

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		int originalOutputBufSize = outputBufs.remainingBytes();
		if (writing == HEADER) {
			outputBufs.add(ByteBuf.wrapForReading(GZIP_HEADER));
			writing = BODY;
		}

		if (writing == BODY) {
			while (!inputBufs.isEmpty()) {
				ByteBuf src = inputBufs.take();
				crc32.update(src.array(), src.readPosition(), src.readRemaining());
				deflater.setInput(src.array(), src.readPosition(), src.readRemaining());
				src.recycle();
				while (!deflater.needsInput()) {
					deflate();
				}
			}

			if (endOfStream) {
				writing = FOOTER;
			}
		}

		if (writing == FOOTER) {
			deflater.finish();
			while (!deflater.finished()) {
				deflate();
			}
			ByteBuf footer = ByteBufPool.allocate(GZIP_FOOTER_SIZE);
			footer.writeInt(Integer.reverseBytes((int) crc32.getValue()));
			footer.writeInt(Integer.reverseBytes(deflater.getTotalIn()));
			outputBufs.add(footer);
			return next.push(outputBufs, true)
					.whenException(this::closeWithError);
		}

		if (originalOutputBufSize == outputBufs.remainingBytes()) {
			return Stage.of(false);
		}

		return next.push(outputBufs, false)
				.whenException(this::closeWithError);
	}

	private void deflate() {
		ByteBuf out = ByteBufPool.allocate(maxBufSize);
		int len = deflater.deflate(out.array(), out.writePosition(), out.writeRemaining());
		if (len > 0) {
			out.writePosition(len);
			outputBufs.add(out);
			return;
		}
		out.recycle();
	}

	@Override
	public void closeWithError(Throwable e) {
		deflater.end();
		outputBufs.recycle();
		next.closeWithError(e);
	}

}
