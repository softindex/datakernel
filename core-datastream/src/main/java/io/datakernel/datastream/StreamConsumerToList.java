package io.datakernel.datastream;

import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static io.datakernel.datastream.StreamCapability.LATE_BINDING;

public final class StreamConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamDataAcceptor<T> {
	private final List<T> list;
	private final SettablePromise<List<T>> resultPromise = new SettablePromise<>();

	private StreamConsumerToList() {
		this(new ArrayList<>());
	}

	private StreamConsumerToList(List<T> list) {
		this.list = list;
	}

	public static <T> StreamConsumerToList<T> create() {
		return new StreamConsumerToList<>();
	}

	public StreamConsumerToList<T> withResultAcceptor(Consumer<Promise<List<T>>> resultAcceptor) {
		resultAcceptor.accept(resultPromise);
		return this;
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
		getSupplier().resume(this);
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		resultPromise.set(list);
		return Promise.complete();
	}

	@Override
	protected void onError(Throwable e) {
		resultPromise.setException(e);
	}

	public Promise<List<T>> getResult() {
		return resultPromise;
	}

	public final List<T> getList() {
		return list;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return EnumSet.of(LATE_BINDING);
	}
}
