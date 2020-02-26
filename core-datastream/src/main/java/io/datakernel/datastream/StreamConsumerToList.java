package io.datakernel.datastream;

import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;

import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates items from this supplier until it closes and
 * then completes the returned promise with a list of those items.
 *
 * @see StreamSupplier#toList()
 */
public final class StreamConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
	private final List<T> list;
	private final SettablePromise<List<T>> resultPromise = new SettablePromise<>();

	{
		resultPromise.whenComplete(this::acknowledge);
		acknowledgement.whenException(resultPromise::trySetException);
	}

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
	protected void onStarted() {
		resume(list::add);
	}

	@Override
	protected void onEndOfStream() {
		resultPromise.trySet(list);
	}

	public Promise<List<T>> getResult() {
		return resultPromise;
	}

	public final List<T> getList() {
		return list;
	}
}
