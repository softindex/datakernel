package io.datakernel.http.stream;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialInput;
import io.datakernel.serial.SerialOutput;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.bytebuf.ByteBufStrings.CR;
import static io.datakernel.bytebuf.ByteBufStrings.LF;
import static io.datakernel.util.Preconditions.checkState;

public final class BufsConsumerChunkedEncoder extends AbstractAsyncProcess
		implements WithSerialToSerial<BufsConsumerChunkedEncoder, ByteBuf, ByteBuf> {
	private final ByteBuf LAST_CHUNK = ByteBuf.wrapForReading(new byte[]{48, 13, 10, 13, 10});

	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	// region creators
	private BufsConsumerChunkedEncoder() {}

	public static BufsConsumerChunkedEncoder create() {
		return new BufsConsumerChunkedEncoder();
	}

	@Override
	public SerialInput<ByteBuf> getInput() {
		return input -> {
			checkState(this.input == null, "Input already set");
			this.input = sanitize(input);
			if (this.input != null && this.output != null) startProcess();
			return getProcessResult();
		};
	}

	@Override
	public SerialOutput<ByteBuf> getOutput() {
		return output -> {
			checkState(this.output == null, "Output already set");
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
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
									.whenResult($ -> doProcess());
						} else {
							buf.recycle();
							doProcess();
						}
					} else {
						output.accept(LAST_CHUNK, null)
								.whenResult($ -> completeProcess());
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
		input.close(e);
		output.close(e);
	}

}
