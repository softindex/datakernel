package io.datakernel.stream.processor;

import io.datakernel.stream.*;

import static io.datakernel.stream.DataStreams.stream;

/**
 * If stream consumer is not immediately wired, on next eventloop cycle it will error out.
 * This is because consumers request producers to start producing items on the next cycle after they're wired.

 * This transformer solves that by storing a data receiver from consumer produce request if it is not wired
 * and when it is actually wired request his new producer to produce into that stored receiver.
 */
public class WaitingStreamConsumer<T> implements StreamTransformer<T, T> {

	private final AbstractStreamProducer<T> output = new AbstractStreamProducer<T>() {
		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			StreamProducer<T> producer = input.getProducer();
			if (producer == null) {
				waitingReceiver = dataReceiver;
				return;
			}
			producer.produce(dataReceiver);
		}

		@Override
		protected void onSuspended() {
			StreamProducer<T> producer = input.getProducer();
			if (producer != null) {
				producer.suspend();
			}
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}
	};

	private final AbstractStreamConsumer<T> input = new AbstractStreamConsumer<T>() {

		@Override
		protected void onStarted() {
			if (waitingReceiver != null) {
				getProducer().produce(waitingReceiver);
			}
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
			output.closeWithError(t);
		}
	};

	private StreamDataReceiver<T> waitingReceiver;

	public static <T> StreamConsumer<T> wrapper(StreamConsumer<T> consumer) {
		WaitingStreamConsumer<T> transformer = new WaitingStreamConsumer<>();
		stream(transformer.output, consumer);
		return transformer.input;
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}
}
