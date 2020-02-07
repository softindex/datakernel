package io.datakernel.datastream;

import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public final class StreamConsumerToList<T> implements StreamConsumer<T>, StreamDataAcceptor<T> {
	private final List<T> list;
	private final SettablePromise<List<T>> resultPromise = new SettablePromise<>();
	private final Promise<Void> acknowledgement = resultPromise.toVoid();

	private StreamConsumerToList(List<T> list) {
		this.list = list;
	}

	public static <T> StreamConsumerToList<T> create() {
		return create(new ArrayList<>());
	}

	public static <T> StreamConsumerToList<T> create(List<T> list) {
		return new StreamConsumerToList<>(list);
	}

	@Override
	public void accept(T item) {
		list.add(item);
	}

	@Override
	public void consume(@NotNull StreamDataSource<T> dataSource) {
		dataSource.resume(list::add);
	}

	@Override
	public void endOfStream() {
		if (resultPromise.isComplete()) return;
		resultPromise.set(list);
	}

	@Override
	public Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	@Override
	public void closeEx(@NotNull Throwable e) {
		resultPromise.trySetException(e);
	}

	public Promise<List<T>> getResult() {
		return resultPromise;
	}

	public final List<T> getList() {
		return list;
	}
}
