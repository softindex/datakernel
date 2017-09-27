package io.datakernel.stream.processor;

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.concurrent.CompletionStage;

public class StreamForwarder<T> {
	private final Eventloop eventloop;

	private final SettableStage<StreamProducer<T>> producerStage;
	private final SettableStage<StreamConsumer<T>> consumerStage;

	private final Input input;
	private final Output output;

	private StreamForwarder(Eventloop eventloop,
	                        SettableStage<StreamProducer<T>> producerStage,
	                        SettableStage<StreamConsumer<T>> consumerStage) {
		this.eventloop = eventloop;
		this.producerStage = producerStage;
		this.consumerStage = consumerStage;
		this.input = new Input(eventloop);
		this.output = new Output(eventloop);
		StreamProducer<T> producer = StreamProducers.ofStage(this.producerStage);
		StreamConsumer<T> consumer = StreamConsumers.ofStage(this.consumerStage);
		producer.streamTo(input);
		output.streamTo(consumer);
	}

	public static <T> StreamForwarder<T> create(Eventloop eventloop) {
		return new StreamForwarder<>(eventloop, SettableStage.create(), SettableStage.create());
	}

	public static <T> StreamForwarder<T> create(Eventloop eventloop,
	                                             SettableStage<StreamProducer<T>> producerStage,
	                                             SettableStage<StreamConsumer<T>> consumerStage) {
		return new StreamForwarder<>(eventloop, producerStage, consumerStage);
	}

	public static <T> StreamForwarder<T> create(Eventloop eventloop,
	                                             CompletionStage<StreamProducer<T>> producerStage,
	                                             CompletionStage<StreamConsumer<T>> consumerStage) {
		return new StreamForwarder<>(eventloop, SettableStage.of(producerStage), SettableStage.of(consumerStage));
	}

	public void setProducer(StreamProducer<T> producer) {
		producerStage.set(producer);
	}

	public void setConsumer(StreamConsumer<T> consumer) {
		consumerStage.set(consumer);
	}

	protected final class Input extends AbstractStreamConsumer<T> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

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
		protected Output(Eventloop eventloop) {
			super(eventloop);
		}

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
