package io.datakernel.stream.processor;

import io.datakernel.async.SettableStage;
import io.datakernel.stream.*;

import java.util.concurrent.CompletionStage;

import static io.datakernel.async.SettableStage.mirrorOf;
import static io.datakernel.stream.DataStreams.bind;

public class StreamForwarder<T> {
	private final SettableStage<StreamProducer<T>> producerStage;
	private final SettableStage<StreamConsumer<T>> consumerStage;

	private final Input input;
	private final Output output;

	private StreamForwarder(SettableStage<StreamProducer<T>> producerStage,
	                        SettableStage<StreamConsumer<T>> consumerStage) {
		this.producerStage = producerStage;
		this.consumerStage = consumerStage;
		this.input = new Input();
		this.output = new Output();
		StreamProducer<T> producer = StreamProducer.ofStage(this.producerStage);
		StreamConsumer<T> consumer = StreamConsumer.ofStage(this.consumerStage);
		bind(producer, input);
		bind(output, consumer);
	}

	public static <T> StreamForwarder<T> create() {
		return new StreamForwarder<>(SettableStage.create(), SettableStage.create());
	}

	public static <T> StreamForwarder<T> create(SettableStage<StreamProducer<T>> producerStage,
	                                            SettableStage<StreamConsumer<T>> consumerStage) {
		return new StreamForwarder<>(producerStage, consumerStage);
	}

	public static <T> StreamForwarder<T> create(CompletionStage<StreamProducer<T>> producerStage,
	                                            CompletionStage<StreamConsumer<T>> consumerStage) {
		return new StreamForwarder<>(mirrorOf(producerStage), mirrorOf(consumerStage));
	}

	public void setProducer(StreamProducer<T> producer) {
		producerStage.set(producer);
	}

	public void setConsumer(StreamConsumer<T> consumer) {
		consumerStage.set(consumer);
	}

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	}

	protected final class Output extends AbstractStreamProducer<T> {
		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			input.getProducer().produce(dataReceiver);
		}
	}

}
