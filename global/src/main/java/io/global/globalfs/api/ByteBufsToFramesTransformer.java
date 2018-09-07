package io.global.globalfs.api;

import io.datakernel.async.AsyncProcess;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.util.Preconditions.checkState;

public abstract class ByteBufsToFramesTransformer implements AsyncProcess, WithSerialToSerial<ByteBufsToFramesTransformer, ByteBuf, DataFrame> {
	protected long position;
	protected long nextCheckpoint;

	protected SerialSupplier<ByteBuf> input;
	protected SerialConsumer<DataFrame> output;

	private SettableStage<Void> process;

	public ByteBufsToFramesTransformer(long offset) {
		this.position = nextCheckpoint = offset;
	}

	@Override
	public void setInput(SerialSupplier<ByteBuf> input) {
		checkState(this.input == null, "Input is already set");
		this.input = input;
	}

	@Override
	public void setOutput(SerialConsumer<DataFrame> output) {
		checkState(this.output == null, "Output is already set");
		this.output = output;
	}

	@Override
	public void closeWithError(Throwable e) {
		if (input != null) {
			input.closeWithError(e);
		}
		if (output != null) {
			output.closeWithError(e);
		}
	}

	protected Stage<Void> postByteBuf(ByteBuf buf) {
		position += buf.readRemaining();
		return output.accept(DataFrame.of(buf));
	}

	protected abstract Stage<Void> postNextCheckpoint();

	protected Stage<Void> handleBuffer(ByteBuf buf) {
		int size = buf.readRemaining();

		if (position + size < nextCheckpoint) {
			return postByteBuf(buf);
		}

		int bytesUntilCheckpoint = (int) (nextCheckpoint - position);
		int remaining = size - bytesUntilCheckpoint;

		if (remaining == 0) {
			return postByteBuf(buf)
					.thenCompose($ -> postNextCheckpoint());
		}

		ByteBuf afterCheckpoint = buf.slice(buf.readPosition() + bytesUntilCheckpoint, remaining);
		buf.recycle(); // set refcount back to 1
		return postByteBuf(buf.slice(bytesUntilCheckpoint))
				.thenCompose($ -> postNextCheckpoint())
				.thenCompose($ -> handleBuffer(afterCheckpoint));
	}

	protected abstract void iteration();

	protected final void finish() {
		process.set(null);
	}

	@Override
	public Stage<Void> process() {
		if (process != null) {
			return process;
		}
		process = new SettableStage<>();
		postNextCheckpoint()
				.thenRun(this::iteration);
		return process;
	}
}
