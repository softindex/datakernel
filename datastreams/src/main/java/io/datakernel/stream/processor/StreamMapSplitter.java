package io.datakernel.stream.processor;

import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static io.datakernel.util.Preconditions.checkState;

public final class StreamMapSplitter<I> implements HasInput<I>, HasOutputs, StreamDataReceiver<I> {
	private final Input input;
	private final List<Output<?>> outputs = new ArrayList<>();
	private final BiConsumer<I, StreamDataReceiver[]> action;

	@SuppressWarnings("unchecked")
	private StreamDataReceiver[] dataReceivers = new StreamDataReceiver[0];
	private int suspended = 0;

	private StreamMapSplitter(BiConsumer<I, StreamDataReceiver[]> action) {
		this.action = action;
		this.input = new Input();
	}

	public static <I> StreamMapSplitter<I> create(BiConsumer<I, StreamDataReceiver[]> action) {
		return new StreamMapSplitter<>(action);
	}

	@SuppressWarnings("unchecked")
	public <O> StreamProducer<O> newOutput() {
		Output output = new Output(outputs.size());
		dataReceivers = Arrays.copyOf(dataReceivers, dataReceivers.length + 1);
		suspended++;
		outputs.add(output);
		return output;
	}

	@Override
	public StreamConsumer<I> getInput() {
		return input;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<? extends StreamProducer<?>> getOutputs() {
		return outputs;
	}

	@Override
	public void onData(I item) {
		action.accept(item, dataReceivers);
	}

	private final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected void onStarted() {
			checkState(!outputs.isEmpty(), "Empty outputs");
		}

		@Override
		protected void onEndOfStream() {
			outputs.forEach(Output::sendEndOfStream);
		}

		@Override
		protected void onError(Throwable t) {
			outputs.forEach(output -> output.closeWithError(t));
		}
	}

	private final class Output<O> extends AbstractStreamProducer<O> {
		private final int index;

		protected Output(int index) {
			this.index = index;
		}

		@Override
		protected void onStarted() {
			checkState(input.getProducer() != null, "Splitter has no input");
		}

		@Override
		protected void onSuspended() {
			suspended++;
			input.getProducer().suspend();
		}

		@Override
		protected void onProduce(StreamDataReceiver<O> dataReceiver) {
			dataReceivers[index] = dataReceiver;
			if (--suspended == 0) {
				input.getProducer().produce(StreamMapSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable t) {
			input.closeWithError(t);
		}
	}
}
