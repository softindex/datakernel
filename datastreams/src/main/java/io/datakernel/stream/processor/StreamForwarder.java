package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

public class StreamForwarder<T> implements StreamTransformer<T, T> {
	private final Eventloop eventloop;

	private Input input;
	private Output output;

	private boolean pendingEndOfStream;
	private Throwable pendingException;
	private StreamDataReceiver<T> pendingDataReceiver;

	private StreamForwarder(Eventloop eventloop) {
		this.eventloop = eventloop;
		this.input = new Input(eventloop);
		this.output = new Output(eventloop);
	}

	public static <T> StreamForwarder<T> create(Eventloop eventloop) {
		return new StreamForwarder<>(eventloop);
	}

	@Override
	public StreamConsumer<T> getInput() {
		if (input == null) {
			input = new Input(eventloop);
			if (pendingException != null) {
				eventloop.post(() -> {
					if (pendingException != null) {
						input.closeWithError(pendingException);
						pendingException = null;
					}
				});
			} else if (pendingDataReceiver != null) {
				eventloop.post(() -> {
					if (pendingDataReceiver != null) {
						input.getProducer().produce(pendingDataReceiver);
						pendingDataReceiver = null;
					}
				});
			}
		}
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		if (output == null) {
			output = new Output(eventloop);
			if (pendingException != null) {
				eventloop.post(() -> {
					if (pendingException != null) {
						output.closeWithError(pendingException);
						pendingException = null;
					}
				});
			} else if (pendingEndOfStream) {
				eventloop.post(() -> {
					if (pendingEndOfStream) {
						output.sendEndOfStream();
						pendingEndOfStream = false;
					}
				});
			}
		}
		return output;
	}

	private class Input extends AbstractStreamConsumer<T> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			if (output != null) {
				output.getConsumer().endOfStream();
			} else {
				pendingEndOfStream = true;
			}
		}

		@Override
		protected void onError(Throwable t) {
			if (output != null) {
				output.getConsumer().closeWithError(t);
			} else {
				pendingException = t;
			}
		}
	}

	private class Output extends AbstractStreamProducer<T> {
		protected Output(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			if (input != null) {
				pendingDataReceiver = null;
				input.getProducer().produce(dataReceiver);
			} else {
				pendingDataReceiver = dataReceiver;
			}
		}

		@Override
		protected void onSuspended() {
			if (input != null) {
				input.getProducer().suspend();
			} else {
				pendingDataReceiver = null;
			}
		}

		@Override
		protected void onError(Throwable t) {
			if (input != null) {
				input.getProducer().closeWithError(t);
			} else {
				pendingException = t;
			}
		}
	}

}
