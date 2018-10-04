package io.datakernel.stream.processor;

import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.stream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static io.datakernel.util.Preconditions.checkState;

public final class StreamMapSplitter<I> implements StreamInput<I>, StreamOutputs, StreamDataAcceptor<I> {
	private final Input input;
	private final List<Output<?>> outputs = new ArrayList<>();
	private final BiConsumer<I, StreamDataAcceptor[]> action;

	@SuppressWarnings("unchecked")
	private StreamDataAcceptor[] dataAcceptors = new StreamDataAcceptor[0];
	private int suspended = 0;

	private StreamMapSplitter(BiConsumer<I, StreamDataAcceptor[]> action) {
		this.action = action;
		this.input = new Input();
	}

	public static <I> StreamMapSplitter<I> create(BiConsumer<I, StreamDataAcceptor[]> action) {
		return new StreamMapSplitter<>(action);
	}

	@SuppressWarnings("unchecked")
	public <O> StreamSupplier<O> newOutput() {
		Output output = new Output(outputs.size());
		dataAcceptors = Arrays.copyOf(dataAcceptors, dataAcceptors.length + 1);
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
	public List<? extends StreamSupplier<?>> getOutputs() {
		return outputs;
	}

	@Override
	public void accept(I item) {
		action.accept(item, dataAcceptors);
	}

	private final class Input extends AbstractStreamConsumer<I> {
		@Override
		protected void onStarted() {
			checkState(!outputs.isEmpty(), "Empty outputs");
		}

		@Override
		protected Stage<Void> onEndOfStream() {
			return Stages.all(outputs.stream().map(Output::sendEndOfStream));
		}

		@Override
		protected void onError(Throwable t) {
			outputs.forEach(output -> output.close(t));
		}
	}

	private final class Output<O> extends AbstractStreamSupplier<O> {
		private final int index;

		protected Output(int index) {
			this.index = index;
		}

		@Override
		protected void onStarted() {
			checkState(input.getSupplier() != null, "Splitter has no input");
		}

		@Override
		protected void onSuspended() {
			suspended++;
			input.getSupplier().suspend();
		}

		@Override
		protected void onProduce(StreamDataAcceptor<O> dataAcceptor) {
			dataAcceptors[index] = dataAcceptor;
			if (--suspended == 0) {
				input.getSupplier().resume(StreamMapSplitter.this);
			}
		}

		@Override
		protected void onError(Throwable t) {
			input.close(t);
		}
	}
}
