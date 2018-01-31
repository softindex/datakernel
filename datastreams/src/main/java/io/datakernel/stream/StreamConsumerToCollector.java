package io.datakernel.stream;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.stream.StreamCapability.TERMINAL;

public final class StreamConsumerToCollector<T, A, R> extends AbstractStreamConsumer<T> implements StreamConsumerWithResult<T, R> {
	private final Collector<T, A, R> collector;
	private final SettableStage<R> resultStage = SettableStage.create();
	private A accumulator;

	public StreamConsumerToCollector(Collector<T, A, R> collector) {
		this.collector = collector;
	}

	@Override
	protected void onStarted() {
		accumulator = collector.supplier().get();
		getProducer().produce(new StreamDataReceiver<T>() {
			private final BiConsumer<A, T> consumer = collector.accumulator();
			private final A accumulator = StreamConsumerToCollector.this.accumulator;

			@Override
			public void onData(T item) {
				consumer.accept(accumulator, item);
			}
		});
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
		return EnumSet.of(LATE_BINDING, TERMINAL);
	}
}
