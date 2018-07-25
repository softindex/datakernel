package io.datakernel.stream;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import java.util.ArrayDeque;
import java.util.Iterator;

import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.util.CollectionUtils.asIterator;
import static io.datakernel.util.Preconditions.checkState;

public final class StreamProducerEndpoint<T> extends AbstractStreamProducer<T> {
	private final ArrayDeque<Object> buffer = new ArrayDeque<>();
	private SettableStage<Void> endOfStream;

	public Stage<Void> post(T item) {
		return post(asIterator(item));
	}

	public Stage<Void> post(Iterable<T> items) {
		return post(items.iterator());
	}

	public Stage<Void> post(Iterator<T> it) {
		assert endOfStream == null;
		if (buffer.isEmpty()) {
			for (; ; ) {
				if (!it.hasNext()) {
					return Stage.of(null);
				}
				StreamDataReceiver<T> dataReceiver = getCurrentDataReceiver();
				if (dataReceiver == null) break;
				dataReceiver.onData(it.next());
			}
		}
		if (getStatus().isOpen()) {
			buffer.add(it);
			SettableStage<Void> stage = new SettableStage<>();
			buffer.add(stage);
			return stage;
		}
		return getStatus() == END_OF_STREAM ? Stage.of(null) : Stage.ofException(getException());
	}

	public Stage<Void> postEndOfStream() {
		checkState(endOfStream == null);
		endOfStream = new SettableStage<>();
		return endOfStream;
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	@Override
	protected void produce(AsyncProduceController async) {
		assert getStatus().isOpen();
		while (!buffer.isEmpty()) {
			Iterator<T> it = (Iterator<T>) buffer.peek();
			while (true) {
				if (it.hasNext()) {
					StreamDataReceiver<T> dataReceiver = getCurrentDataReceiver();
					if (dataReceiver == null) break;
					dataReceiver.onData(it.next());
				} else {
					buffer.poll();
					SettableStage<Void> stage = (SettableStage<Void>) buffer.poll();
					stage.set(null);
				}
			}
		}
		if (endOfStream != null && buffer.isEmpty()) {
			sendEndOfStream();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void onError(Throwable t) {
	}

	@SuppressWarnings({"ConstantConditions", "unchecked"})
	@Override
	protected void cleanup() {
		while (!buffer.isEmpty()) {
			Object value = buffer.poll();
			SettableStage<Void> stage = (SettableStage<Void>) buffer.poll();
			stage.set(null, getException());
		}
		if (endOfStream != null) {
			SettableStage<Void> endOfStream = this.endOfStream;
			this.endOfStream = null;
			endOfStream.set(null, getException());
		}
	}
}
