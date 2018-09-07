package io.datakernel.remotefs;

import io.datakernel.async.AsyncProcess;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.processor.WithSerialToSerial;

import static io.datakernel.util.Preconditions.checkState;

public class SerialByteBufCutter implements AsyncProcess, WithSerialToSerial<SerialByteBufCutter, ByteBuf, ByteBuf> {
	private final long offset;

	private long position = 0;

	private SerialSupplier<ByteBuf> input;
	private SerialConsumer<ByteBuf> output;

	private SettableStage<Void> process;

	// region creators
	private SerialByteBufCutter(long offset) {
		this.offset = offset;
	}

	public static SerialByteBufCutter create(long offset) {
		return new SerialByteBufCutter(offset);
	}

	@Override
	public Stage<Void> process() {
		checkState(input != null, "Input was not set");
		checkState(output != null, "Input was not set");
		if (process != null) {
			return process;
		}
		process = new SettableStage<>();
		doProcess();
		return process;
	}

	private void doProcess() {
		input.get()
				.async()
				.whenComplete((item, e) -> {
					if (e != null) {
						closeWithError(e);
						return;
					}
					if (item == null) {
						output.accept(null);
						process.set(null);
						return;
					}
					int size = item.readRemaining();
					position += size;
					if (position <= offset) {
						item.recycle();
						doProcess();
						return;
					}
					if (position - size < offset) {
						item.moveReadPosition(size - (int) (position - offset));
					}
					output.accept(item)
							.whenComplete(($, e2) -> {
								if (e2 != null) {
									closeWithError(e2);
								} else {
									doProcess();
								}
							});
				});
	}

	@Override
	public void closeWithError(Throwable e) {
		if (input != null) {
			input.closeWithError(e);
		}
		if (output != null) {
			output.closeWithError(e);
		}
		process.trySetException(e);
	}

	@Override
	public void setInput(SerialSupplier<ByteBuf> input) {
		checkState(this.input == null, "Input already set");
		this.input = input;
	}

	@Override
	public void setOutput(SerialConsumer<ByteBuf> output) {
		checkState(this.output == null, "Output already set");
		this.output = output;
	}
}
