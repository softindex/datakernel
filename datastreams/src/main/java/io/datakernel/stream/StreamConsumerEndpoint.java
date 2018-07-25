package io.datakernel.stream;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import java.util.ArrayDeque;

import static io.datakernel.stream.StreamStatus.END_OF_STREAM;

public final class StreamConsumerEndpoint<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
	private final ArrayDeque<T> buffer = new ArrayDeque<>();
	private final ArrayDeque<SettableStage<T[]>> stages = new ArrayDeque<>();

	private int maxBufferSize = 0;

	public StreamConsumerEndpoint<T> withMaxBufferSize(int maxBufferSize) {
		this.maxBufferSize = maxBufferSize;
		return this;
	}

	@Override
	protected void onStarted() {
		if (buffer.size() < maxBufferSize) {
			getProducer().produce(this);
		}
	}

	@Override
	protected void onEndOfStream() {
		while (true) {
			SettableStage<T[]> stage = stages.poll();
			if (stage == null) break;
			stage.set(null);
		}
	}

	@Override
	protected void onError(Throwable t) {
		while (true) {
			SettableStage<T[]> stage = stages.poll();
			if (stage == null) break;
			stage.setException(t);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onData(T item) {
		if (stages.isEmpty()) {
			buffer.add(item);
			if (buffer.size() >= maxBufferSize) {
				getProducer().suspend();
			}
		} else {
			SettableStage<T[]> stage = stages.poll();
			stage.set((T[]) new Object[]{item});
		}
	}

	public Stage<T[]> receive() {
		return receive(Integer.MAX_VALUE);
	}

	@SuppressWarnings("unchecked")
	public Stage<T[]> receive(int maxCount) {
		if (!buffer.isEmpty()) {
			if (maxCount >= buffer.size()) {
				return Stage.of(buffer.toArray((T[]) new Object[0]));
			}
			T[] result = (T[]) new Object[maxCount];
			for (int i = 0; i < maxCount; i++) {
				result[i] = buffer.pollFirst();
			}
			return Stage.of(result);
		}
		if (getStatus().isOpen()) {
			if (stages.isEmpty()) {
				getProducer().produce(this);
			}
			SettableStage<T[]> stage = new SettableStage<>();
			stages.add(stage);
			return stage;
		}
		return getStatus() == END_OF_STREAM ? Stage.of(null) : Stage.ofException(getException());
	}
}
