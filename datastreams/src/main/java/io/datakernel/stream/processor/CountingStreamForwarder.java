package io.datakernel.stream.processor;

import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.Collection;

public class CountingStreamForwarder<T> implements StreamTransformer<T, T>, StreamDataReceiver<T> {
	private final Eventloop eventloop;
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
	}

	private CountingStreamForwarder(Eventloop eventloop, SizeCounter<T> sizeCounter) {
		this.eventloop = eventloop;
		this.sizeCounter = sizeCounter;
		this.input = new Input(eventloop);
		this.output = new Output(eventloop);
	}

	public static <T> CountingStreamForwarder<T> create(Eventloop eventloop, SizeCounter<T> sizeCounter) {
		return new CountingStreamForwarder<T>(eventloop, sizeCounter);
	}

	public static CountingStreamForwarder<ByteBuf> forByteBufs(Eventloop eventloop) {
		return create(eventloop, ByteBuf::readRemaining);
	}

	public static <T extends Collection<?>> CountingStreamForwarder<T> forCollections(Eventloop eventloop) {
		return create(eventloop, Collection::size);
	}

	public static <T> CountingStreamForwarder<T> create(Eventloop eventloop) {
		return new CountingStreamForwarder<T>(eventloop, null);
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

	private class Input extends AbstractStreamConsumer<T> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			output.getConsumer().endOfStream();
			resultStage.set(new Counter(count, size));
		}

		@Override
		protected void onError(Exception e) {
			output.getConsumer().closeWithError(e);
			resultStage.setException(e);
		}
	}

	private class Output extends AbstractStreamProducer<T> {
		protected Output(Eventloop eventloop) {
			super(eventloop);
		}

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
		protected void onError(Exception e) {
			input.getProducer().closeWithError(e);
		}
	}

	public long getCount() {
		return count;
	}

	public long getSize() {
		return size;
	}
}
