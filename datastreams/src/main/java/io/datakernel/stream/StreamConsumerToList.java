package io.datakernel.stream;

import io.datakernel.async.SettableStage;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import static io.datakernel.util.Preconditions.checkNotNull;

public class StreamConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, List<T>>, StreamDataReceiver<T> {
	protected final List<T> list;
	private final SettableStage<List<T>> resultStage = SettableStage.create();

	public StreamConsumerToList() {
		this(new ArrayList<>());
	}

	public StreamConsumerToList(List<T> list) {
		checkNotNull(list);
		this.list = list;
	}

	public static <T> StreamConsumerToList<T> create(List<T> list) {
		return new StreamConsumerToList<>(list);
	}

	public static <T> StreamConsumerToList<T> create() {
		return new StreamConsumerToList<>(new ArrayList<>());
	}

	public static <T> StreamConsumerToList<T> oneByOne(List<T> list) {
		return new StreamConsumerToList<T>(list) {
			@Override
			public void onData(T item) {
				list.add(item);
				getProducer().suspend();
				this.eventloop.post(() -> getProducer().produce(this));
			}
		};
	}

	public static <T> StreamConsumerToList<T> oneByOne() {
		return oneByOne(new ArrayList<T>());
	}

	public static <T> StreamConsumerToList<T> randomlySuspending(List<T> list, final Random random) {
		return new StreamConsumerToList<T>(list) {
			@Override
			public void onData(T item) {
				list.add(item);
				if (random.nextBoolean()) {
					getProducer().suspend();
					this.eventloop.post(() -> getProducer().produce(this));
				}
			}
		};
	}

	public static <T> StreamConsumerToList<T> randomlySuspending(List<T> list) {
		return randomlySuspending(list, new Random());
	}

	public static <T> StreamConsumerToList<T> randomlySuspending() {
		return randomlySuspending(new ArrayList<T>(), new Random());
	}

	@Override
	public void onData(T item) {
		list.add(item);
	}

	@Override
	protected void onStarted() {
		getProducer().produce(this);
	}

	@Override
	protected void onEndOfStream() {
		if (resultStage != null) {
			resultStage.set(list);
		}
	}

	@Override
	protected void onError(Throwable t) {
		if (resultStage != null) {
			resultStage.setException(t);
		}
	}

	@Override
	public final CompletionStage<List<T>> getResult() {
		return resultStage;
	}

	public final List<T> getList() {
		return list;
	}
}
