package io.datakernel.stream;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumerToCollector<T, A, R> extends AbstractStreamConsumer<T> implements StreamConsumer<T> {
	private final Collector<T, A, R> collector;
	private final SettablePromise<R> resultPromise = new SettablePromise<>();
	private A accumulator;

	public StreamConsumerToCollector(Collector<T, A, R> collector) {
		this.collector = collector;
	}

	@Override
	protected void onStarted() {
		accumulator = collector.supplier().get();
		BiConsumer<A, T> consumer = collector.accumulator();
		getSupplier().resume(item -> consumer.accept(accumulator, item));
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		R result = collector.finisher().apply(accumulator);
		accumulator = null;
		resultPromise.set(result);
		return Promise.complete();
	}

	@Override
	protected void onError(Throwable t) {
		resultPromise.setException(t);
	}

	public MaterializedPromise<R> getResult() {
		return resultPromise;
	}

	public A getAccumulator() {
		return accumulator;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return EnumSet.of(LATE_BINDING);
	}
}
