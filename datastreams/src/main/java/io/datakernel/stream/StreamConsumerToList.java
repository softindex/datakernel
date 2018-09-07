package io.datakernel.stream;

import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamConsumer<T>, StreamDataReceiver<T> {
	protected final List<T> list;
	private final SettableStage<List<T>> resultStage = new SettableStage<>();

	private StreamConsumerToList() {
		this(new ArrayList<>());
	}

	private StreamConsumerToList(List<T> list) {
		this.list = list;
	}

	public static <T> StreamConsumerToList<T> create() {
		return new StreamConsumerToList<>();
	}

	public StreamConsumerToList<T> withResultAcceptor(Consumer<Stage<List<T>>> resultAcceptor) {
		resultAcceptor.accept(resultStage);
		return this;
	}

	public static <T> StreamConsumerToList<T> create(List<T> list) {
		return new StreamConsumerToList<>(list);
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
	protected Stage<Void> onProducerEndOfStream() {
		resultStage.set(list);
		return Stage.complete();
	}

	@Override
	protected void onError(Throwable t) {
		resultStage.setException(t);
	}

	public MaterializedStage<List<T>> getResult() {
		return resultStage;
	}

	public final List<T> getList() {
		return list;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return EnumSet.of(LATE_BINDING);
	}
}
