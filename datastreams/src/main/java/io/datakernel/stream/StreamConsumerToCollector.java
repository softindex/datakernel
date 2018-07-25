package io.datakernel.stream;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumerToCollector<T, A, R> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, R> {
	private final Collector<T, A, R> collector;
	private final SettableStage<R> resultStage = new SettableStage<>();
	private A accumulator;

	public StreamConsumerToCollector(Collector<T, A, R> collector) {
		this.collector = collector;
	}

	@Override
	protected void onStarted() {
		accumulator = collector.supplier().get();
		BiConsumer<A, T> consumer = collector.accumulator();
		getProducer().produce(item -> consumer.accept(accumulator, item));
	}

	@Override
	protected void onEndOfStream() {
		R result = collector.finisher().apply(accumulator);
		accumulator = null;
		resultStage.set(result);
	}

	@Override
	protected void onError(Throwable t) {
		resultStage.setException(t);
	}

	@Override
	public Stage<R> getResult() {
		return resultStage;
	}

	public A getAccumulator() {
		return accumulator;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return EnumSet.of(LATE_BINDING);
	}
}
