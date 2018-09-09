package io.datakernel.http.stream2;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;

public final class BufsConsumerChunkedEncoder extends AbstractAsyncProcess
		implements WithSerialToSerial<BufsConsumerChunkedEncoder, ByteBuf, ByteBuf> {
	private static final byte[] LAST_CHUNK = {48, 13, 10, 13, 10};

	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	@Override
	public void setInput(SerialSupplier<ByteBuf> input) {
		this.input = input;
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		this.output = output;
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenComplete((buf, e1) -> {
					if (isProcessComplete()) {
						if (buf != null) buf.recycle();
						return;
					}
					if (e1 == null) {
						if (buf != null) {
							output.accept(encodeBuf(buf))
									.whenComplete(($, e2) -> {
										if (isProcessComplete()) return;
										if (e2 == null) {
											doProcess();
										} else {
											closeWithError(e2);
										}
									});
						} else {
							Stage.complete()
									.thenCompose($ -> output.accept(getLastChunk()))
									.thenCompose($ -> output.accept(null))
									.whenComplete(($, e2) -> {
										if (isProcessComplete()) return;
										if (e2 == null) {
											completeProcess();
										} else {
											closeWithError(e2);
										}
									});
						}
					} else {
						closeWithError(e1);
					}
				});
	}

	private static ByteBuf getLastChunk() {
		ByteBuf endOfStreamBuf = ByteBufPool.allocate(5);
		// writing "0\r\n\r\n" aka last-chunk
		endOfStreamBuf.write(LAST_CHUNK);
		endOfStreamBuf.writePosition(5);
		return endOfStreamBuf;
	}

	private static ByteBuf encodeBuf(ByteBuf buf) {
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
	protected void doCloseWithError(Throwable e) {
		input.closeWithError(e);
		output.closeWithError(e);
	}

}
