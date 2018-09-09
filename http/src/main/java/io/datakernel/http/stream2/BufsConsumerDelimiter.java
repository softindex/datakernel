package io.datakernel.http.stream2;

import io.datakernel.async.AbstractAsyncProcess;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.processor.WithByteBufsInput;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.serial.SerialSuppliers.sendByteBufQueue;

public final class BufsConsumerDelimiter extends AbstractAsyncProcess
		implements WithSerialToSerial<BufsConsumerDelimiter, ByteBuf, ByteBuf>, WithByteBufsInput<BufsConsumerDelimiter> {

	private ByteBufsSupplier input;
	private SerialConsumer<ByteBuf> output;

	private int remaining;

	private BufsConsumerDelimiter(int remaining) {this.remaining = remaining;}

	public static BufsConsumerDelimiter create(int remaining) {
		return new BufsConsumerDelimiter(remaining);
	}

	@Override
	public void setInput(ByteBufsSupplier input) {
		this.input = input;
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		this.output = output;
	}

	@Override
	protected void doProcess() {
		if (remaining != 0) {
			input.get()
					.whenComplete(($1, e1) -> {
						if (isProcessComplete()) return;
						if (e1 == null) {
							ByteBufQueue outputBufs = new ByteBufQueue();
							remaining -= input.bufs.drainTo(outputBufs, remaining);
							sendByteBufQueue(outputBufs, output)
									.whenComplete(($2, e2) -> {
										if (isProcessComplete()) return;
										if (e2 == null) {
											doProcess();
										} else {
											closeWithError(e2);
										}
									});
						} else {
							closeWithError(e1);
						}
					});
		} else {
			input.markEndOfStream()
					.whenComplete(($1, e1) -> {
						if (isProcessComplete()) return;
						if (e1 == null) {
							output.accept(null)
									.whenComplete(($2, e2) -> {
										if (isProcessComplete()) return;
										if (e2 == null) {
											completeProcess();
										} else {
											closeWithError(e1);
										}
									});
						} else {
							closeWithError(e1);
						}
					});
		}
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.closeWithError(e);
		output.closeWithError(e);
	}
}
