package io.datakernel.http.stream2;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.serial.ByteBufsSupplier;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.processor.AbstractIOAsyncProcess;
import io.datakernel.serial.processor.WithByteBufsInput;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.util.Preconditions.checkState;

public final class BufsConsumerDelimiter extends AbstractIOAsyncProcess
		implements WithSerialToSerial<BufsConsumerDelimiter, ByteBuf, ByteBuf>, WithByteBufsInput<BufsConsumerDelimiter> {

	private ByteBufQueue bufs;
	private ByteBufsSupplier input;
	private SerialConsumer<ByteBuf> output;

	private int remaining;

	// region creators
	private BufsConsumerDelimiter(int remaining) {this.remaining = remaining;}

	public static BufsConsumerDelimiter create(int remaining) {
		checkState(remaining >= 0, "Cannot create delimiter with number of remaining bytes that is less than 0");
		return new BufsConsumerDelimiter(remaining);
	}

	@Override
	public void setInput(ByteBufsSupplier input) {
		checkState(this.input == null, "Input already set");
		this.input = sanitize(input);
		this.bufs = input.bufs;
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
		if (remaining == 0) {
			input.endOfStream()
					.thenCompose($2 -> output.accept(null))
					.thenRun(this::completeProcess);
			return;
		}
		ByteBufQueue outputBufs = new ByteBufQueue();
		remaining -= bufs.drainTo(outputBufs, remaining);
		output.acceptAll(outputBufs.asIterator())
				.whenResult($ -> {
					if (remaining != 0) {
						input.needMoreData()
								.thenRun(this::doProcess);
					} else {
						input.endOfStream()
								.thenCompose($2 -> output.accept(null))
								.thenRun(this::completeProcess);
					}
				});
	}

	@Override
	protected void doCloseWithError(Throwable e) {
		input.closeWithError(e);
		output.closeWithError(e);
	}
}
