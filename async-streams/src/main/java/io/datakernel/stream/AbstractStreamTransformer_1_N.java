/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.stream;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a {@link AbstractStreamConsumer} with several {@link AbstractStreamProducer} .
 *
 * @param <I> type of receiving items
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_1_N<I> implements StreamConsumer<I> {
	protected final AbstractStreamConsumer<I> internalConsumer;
	protected final List<AbstractStreamProducer<?>> internalProducers = new ArrayList<>();
	protected final Eventloop eventloop;

	protected void onDataReceiverChanged(int outputIndex) {
	}

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this consumer will run
	 */
	public AbstractStreamTransformer_1_N(Eventloop eventloop) {
		this.eventloop = eventloop;
		this.internalConsumer = new AbstractStreamConsumer<I>(eventloop) {
			@Override
			public StreamDataReceiver<I> getDataReceiver() {
				return AbstractStreamTransformer_1_N.this.getInternalDataReceiver();
			}

			@Override
			public void onProducerEndOfStream() {
				AbstractStreamTransformer_1_N.this.onUpstreamProducerEndOfStream();
			}
		};
	}

	protected abstract void onUpstreamProducerEndOfStream();

	protected abstract StreamDataReceiver<I> getInternalDataReceiver();

	protected <T extends AbstractStreamProducer<?>> T addOutput(T newOutput) {
		internalProducers.add(newOutput);
		return newOutput;
	}

	@Override
	public final void onProducerError(Exception e) {
		closeDownstreamsWithError(e);
		internalConsumer.onProducerError(e);
	}

	/**
	 * Returns all producers from this consumer
	 */
	public int getOutputsCount() {
		return internalProducers.size();
	}

	/**
	 * Checks if all producers of this consumer are ready
	 *
	 * @return true if all are ready, false else
	 */
	protected boolean allOutputsResumed() {
		for (AbstractStreamProducer<?> output : internalProducers) {
			if (output.getDownstream() == null)
				return false;
			if (output.getStatus() != AbstractStreamProducer.READY) {
				return false;
			}
		}
		return true;
	}

	protected void sendEndOfStreamToDownstreams() {
		for (AbstractStreamProducer<?> output : internalProducers) {
			output.getDownstream().onProducerEndOfStream();
		}
	}

	protected void closeDownstreamsWithError(Exception e) {
		for (AbstractStreamProducer<?> output : internalProducers) {
			output.getDownstream().onProducerError(e);
		}
	}

	@Override
	public final StreamDataReceiver<I> getDataReceiver() {
		return internalConsumer.getDataReceiver();
	}

	@Override
	public final void setUpstream(StreamProducer<I> upstreamProducer) {
		internalConsumer.setUpstream(upstreamProducer);
	}

	@Override
	public final StreamProducer<I> getUpstream() {
		return internalConsumer.getUpstream();
	}

	@Override
	public final void onProducerEndOfStream() {
		sendEndOfStreamToDownstreams();
	}

	@Override
	public final void addConsumerCompletionCallback(CompletionCallback completionCallback) {
		internalConsumer.addConsumerCompletionCallback(completionCallback);
	}
}
