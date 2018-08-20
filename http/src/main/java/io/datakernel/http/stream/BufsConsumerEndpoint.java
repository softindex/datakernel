package io.datakernel.http.stream;

import io.datakernel.serial.SerialBuffer;
import io.datakernel.async.Stage;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;

public final class BufsConsumerEndpoint implements BufsConsumer {
	private final SerialBuffer<ByteBuf> bridge;

	public BufsConsumerEndpoint(int bufferSize) {
		bridge = new SerialBuffer<>(bufferSize);
	}

	@Override
	public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
		if (inputBufs.isEmpty()) {
			return endOfStream ?
					bridge.put(null).thenApply($ -> true) :
					Stage.of(false);
		}
		return bridge.put(inputBufs.take())
				.thenCompose($ -> push(inputBufs, endOfStream));
	}

	public SerialSupplier<ByteBuf> getSupplier() {
		return bridge.getSupplier();
	}

	public void skipRemaining() {
		if (!bridge.isPendingTake()) {
			skipRemainingImpl(bridge);
		}
	}

	private static void skipRemainingImpl(SerialBuffer<ByteBuf> bridge) {
		bridge.take()
				.whenResult(buf -> {
					if (buf != null) {
						buf.recycle();
						skipRemainingImpl(bridge);
					}
				});
	}

	@Override
	public void closeWithError(Throwable e) {
		bridge.closeWithError(e);
	}
}
