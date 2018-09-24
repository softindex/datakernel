package io.datakernel.http.stream2;

import io.datakernel.async.MaterializedStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.AbstractIOAsyncProcess;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.util.Preconditions.checkState;

public final class BufsConsumerChunkedEncoder extends AbstractIOAsyncProcess
		implements WithSerialToSerial<BufsConsumerChunkedEncoder, ByteBuf, ByteBuf> {
	private final ByteBuf LAST_CHUNK = ByteBuf.wrapForReading(new byte[]{48, 13, 10, 13, 10});

	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerChunkedEncoder() {}

	public static BufsConsumerChunkedEncoder create(){
		return new BufsConsumerChunkedEncoder();
	}

	@Override
	public MaterializedStage<Void> setInput(SerialSupplier<ByteBuf> input) {
		checkState(this.input == null, "Input already set");
		this.input = sanitize(input);
		return getResult();
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		checkState(this.output == null, "Output already set");
		this.output = sanitize(output);
	}
	// endregion

	@Override
	protected void beforeProcess() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Output was not set");
	}

	@Override
	protected void doProcess() {
		input.get()
				.whenResult(buf -> {
						if (buf != null) {
							if (buf.canRead()) {
								output.accept(encodeBuf(buf))
										.thenRun(this::doProcess);
							} else {
								buf.recycle();
								doProcess();
							}
						} else {
							output.accept(LAST_CHUNK, null)
									.thenRun(this::completeProcess);
						}
				});
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
