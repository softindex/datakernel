package io.datakernel.datastream.processor;

import io.datakernel.datastream.StreamDataAcceptor;

public abstract class AbstractTransducer<I, O, A> implements Transducer<I, O, A> {
	@Override
	public A onStarted(StreamDataAcceptor<O> output) {
		return null;
	}

	@Override
	public void onEndOfStream(StreamDataAcceptor<O> output, A accumulator) {
	}

	@Override
	public boolean isOneToMany() {
		return false;
	}
}
