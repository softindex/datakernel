package io.datakernel.stream;

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public class StreamConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, List<T>>, StreamDataReceiver<T> {
	protected final List<T> list;
	private final SettableStage<List<T>> resultStage = SettableStage.create();

	public StreamConsumerToList(Eventloop eventloop) {
		this(eventloop, new ArrayList<>());
	}

	public StreamConsumerToList(Eventloop eventloop, List<T> list) {
		super(eventloop);
		checkNotNull(list);
		this.list = list;
	}

	public static <T> StreamConsumerToList<T> create(final Eventloop eventloop, List<T> list) {
		return new StreamConsumerToList<>(eventloop, list);
	}

	public static <T> StreamConsumerToList<T> create(final Eventloop eventloop) {
		return new StreamConsumerToList<>(eventloop, new ArrayList<>());
	}

	public static <T> StreamConsumerToList<T> oneByOne(final Eventloop eventloop, List<T> list) {
		return new StreamConsumerToList<T>(eventloop, list) {
			@Override
			public void onData(T item) {
				list.add(item);
				getProducer().suspend();
				this.eventloop.post(() -> getProducer().produce(this));
			}
		};
	}

	public static <T> StreamConsumerToList<T> oneByOne(Eventloop eventloop) {
		return oneByOne(eventloop, new ArrayList<T>());
	}

	public static <T> StreamConsumerToList<T> randomlySuspending(Eventloop eventloop, List<T> list, final Random random) {
		return new StreamConsumerToList<T>(eventloop, list) {
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

	public static <T> StreamConsumerToList<T> randomlySuspending(Eventloop eventloop, List<T> list) {
		return randomlySuspending(eventloop, list, new Random());
	}

	public static <T> StreamConsumerToList<T> randomlySuspending(Eventloop eventloop) {
		return randomlySuspending(eventloop, new ArrayList<T>(), new Random());
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
		checkState(getStatus().isClosed());
		return list;
	}
}
