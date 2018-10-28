package io.datakernel.stream;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamConsumer<T>, StreamDataAcceptor<T> {
	protected final List<T> list;
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
	protected void onError(Throwable t) {
		resultPromise.setException(t);
	}

	public MaterializedPromise<List<T>> getResult() {
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
