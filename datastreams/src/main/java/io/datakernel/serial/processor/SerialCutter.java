package io.datakernel.serial.processor;

import io.datakernel.async.AsyncProcess;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;

import static io.datakernel.util.Preconditions.checkState;

public class SerialCutter<T> implements AsyncProcess, WithSerialToSerial<SerialCutter<T>, T, T> {
	private final SizeCounter<T> sizeCounter;
	private final Slicer<T> slicer;
	private final long offset;
	private final long endOffset;

	private long position = 0;

	private SerialSupplier<T> input;
	private SerialConsumer<T> output;

	private SettableStage<Void> process;

	// region creators
	private SerialCutter(long offset, long length, SizeCounter<T> sizeCounter, Slicer<T> slicer) {
		this.offset = offset;
		this.slicer = slicer;
		this.endOffset = length == -1 ? Long.MAX_VALUE : offset + length;
		this.sizeCounter = sizeCounter;
	}

	public static <T> SerialCutter<T> create(long offset, long length, SizeCounter<T> sizeCounter, Slicer<T> slicer) {
		return new SerialCutter<>(offset, length, sizeCounter, slicer);
	}

	public static <T> SerialCutter<T> create(long offset, long length) {
		return new SerialCutter<>(offset, length, $ -> 1, (item, off, len) -> item);
	}

	public static <T> SerialCutter<T> create(long offset, long length, SliceStrategy<T> sliceStrategy) {
		return new SerialCutter<>(offset, length, sliceStrategy.sizeCounter, sliceStrategy.slicer);
	}

	public static <T> SerialCutter<T> create(long offset, SizeCounter<T> sizeCounter, Slicer<T> slicer) {
		return new SerialCutter<>(offset, -1, sizeCounter, slicer);
	}

	public static <T> SerialCutter<T> create(long offset) {
		return new SerialCutter<>(offset, -1, $ -> 1, (item, off, len) -> item);
	}

	public static <T> SerialCutter<T> create(long offset, SliceStrategy<T> sliceStrategy) {
		return new SerialCutter<>(offset, -1, sliceStrategy.sizeCounter, sliceStrategy.slicer);
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
					int size = sizeCounter.sizeOf(item);
					position += size;
					if (position <= offset || position - size > endOffset) {
						doProcess();
						return;
					}
					if (position - size < offset) {
						int cut = (int) (position - offset);
						item = slicer.slice(item, size - cut, cut);
					}
					if (position > endOffset) {
						item = slicer.slice(item, 0, (int) (endOffset - position) + size);
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
	public void setInput(SerialSupplier<T> input) {
		checkState(this.input == null, "Input already set");
		this.input = input;
	}

	@Override
	public void setOutput(SerialConsumer<T> output) {
		checkState(this.output == null, "Output already set");
		this.output = output;
	}

	/**
	 * Represents offset strategy to be used when skipping data elements.
	 *
	 * @param <T>
	 */
	public static final class SliceStrategy<T> {
		private final SizeCounter<T> sizeCounter;
		private final Slicer<T> slicer;

		public SliceStrategy(SizeCounter<T> sizeCounter, Slicer<T> slicer) {
			this.sizeCounter = sizeCounter;
			this.slicer = slicer;
		}

		public static SliceStrategy<ByteBuf> forByteBuf() {
			return new SliceStrategy<>(ByteBuf::readRemaining, (buf, offset, length) -> {
				buf.moveReadPosition(offset);
				buf.moveWritePosition(length - buf.readRemaining());
				return buf;
			});
		}

		public static SliceStrategy<String> forString() {
			return new SliceStrategy<>(String::length, (str, offset, length) -> str.substring(offset, offset + length));
		}
	}

	@FunctionalInterface
	public interface SizeCounter<T> {
		int sizeOf(T object);
	}

	@FunctionalInterface
	public interface Slicer<T> {
		T slice(T object, int offset, int length);
	}
}
