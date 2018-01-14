package io.datakernel.stream.processor;

import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.stream.*;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

public class CountingStreamForwarder<T> implements StreamTransformer<T, T>, StreamDataReceiver<T> {
	private final SizeCounter<T> sizeCounter;

	private final Input input;
	private final Output output;
	private final SettableStage<Counter> resultStage = SettableStage.create();

	private long count;
	private long size;
	private StreamDataReceiver<T> lastDataReceiver;

	public static final class Counter {
		private final long count;
		private final long size;

		private Counter(long count, long size) {
			this.count = count;
			this.size = size;
		}

		public long getCount() {
			return count;
		}

		public long getSize() {
			return size;
		}
	}

	private CountingStreamForwarder(SizeCounter<T> sizeCounter) {
		this.sizeCounter = sizeCounter;
		this.input = new Input();
		this.output = new Output();
	}

	public static <T> CountingStreamForwarder<T> create(SizeCounter<T> sizeCounter) {
		return new CountingStreamForwarder<>(sizeCounter);
	}

	public static CountingStreamForwarder<ByteBuf> forByteBufs() {
		return create(ByteBuf::readRemaining);
	}

	public static <T extends Collection<?>> CountingStreamForwarder<T> forCollections() {
		return create(Collection::size);
	}

	public static <T> CountingStreamForwarder<T> create() {
		return new CountingStreamForwarder<>(null);
	}

	@Override
	public void onData(T item) {
		count++;
		if (sizeCounter != null) {
			size += sizeCounter.size(item);
		}
		lastDataReceiver.onData(item);
	}

	public interface SizeCounter<T> {
		int size(T item);
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

	public CompletionStage<Counter> getResult() {
		return resultStage;
	}

	private class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
			resultStage.set(new Counter(count, size));
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
			resultStage.setException(t);
		}
	}

	private class Output extends AbstractStreamProducer<T> {
		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			CountingStreamForwarder.this.lastDataReceiver = dataReceiver;
			input.getProducer().produce(CountingStreamForwarder.this);
		}

		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}
	}

	public long getCount() {
		return count;
	}

	public long getSize() {
		return size;
	}
}
