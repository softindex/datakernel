package io.datakernel.http.stream;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;

import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class BufsConsumerGunzip implements BufsConsumer {
	private final BufsConsumer next;

	private final Inflater inflater;
	private final int size;

	private final ByteBufQueue outputBufs = new ByteBufQueue();
	private final CRC32 crc32 = new CRC32();

	public BufsConsumerGunzip(BufsConsumer next,
			Inflater inflater, int size) {
		this.next = next;
		this.inflater = inflater;
		this.size = size;
	}

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		while (!inputBufs.isEmpty()) {
			ByteBuf src = inputBufs.take();

			crc32.update(src.array(), src.readPosition(), src.readRemaining());

			for (; ; ) {
				if (inflater.needsInput()) {
					if (src.readRemaining() == 0) {
						src.recycle();
						break;
					}

					int part = Math.max(src.readRemaining(), 512);
					inflater.setInput(src.array(), src.readPosition(), part);
					src.moveReadPosition(part);
				}

				for (; ; ) {
					ByteBuf buf = ByteBufPool.allocate(size);
					int n;
					try {
						n = inflater.inflate(buf.array(), 0, buf.writeRemaining());
					} catch (DataFormatException e) {
						buf.recycle();
						throw new RuntimeException(e); // TODO
					}
					if (n == 0) {
						buf.recycle();
						break;
					}
					outputBufs.add(buf);
				}

			}
		}

		return next.push(outputBufs, false);
	}

	@Override
	public void closeWithError(Throwable e) {
		inflater.end();
		outputBufs.clear();
		next.closeWithError(e);
	}

}
