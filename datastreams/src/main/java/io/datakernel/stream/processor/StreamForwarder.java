package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

public class StreamForwarder<T> implements StreamTransformer<T, T> {
	private final Eventloop eventloop;

	private Input input;
	private Output output;

	private boolean pendingEndOfStream;
	private Exception pendingException;
	private StreamDataReceiver<T> pendingDataReceiver;

	private StreamForwarder(Eventloop eventloop) {
		this.eventloop = eventloop;
		this.input = new Input(eventloop);
		this.output = new Output(eventloop);
	}

	public static <T> StreamForwarder<T> create(Eventloop eventloop, SizeCounter<T> sizeCounter) {
		return new StreamForwarder<T>(eventloop);
	}

	public static <T> StreamForwarder<T> create(Eventloop eventloop) {
		return new StreamForwarder<T>(eventloop);
	}

	public interface SizeCounter<T> {
		int size(T item);
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
		protected void onError(Exception e) {
			if (output != null) {
				output.getConsumer().closeWithError(e);
			} else {
				pendingException = e;
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
		protected void onError(Exception e) {
			if (input != null) {
				input.getProducer().closeWithError(e);
			} else {
				pendingException = e;
			}
		}
	}

}
