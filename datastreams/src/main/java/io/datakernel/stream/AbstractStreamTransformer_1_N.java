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

import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.arraycopy;
import static java.util.Collections.unmodifiableList;

/**
 * Represents a {@link AbstractStreamConsumer} with several {@link AbstractStreamProducer} .
 *
 * @param <I> type of receiving items
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_1_N<I> implements HasInput<I>, HasOutputs {
	protected final Eventloop eventloop;

	protected AbstractInputConsumer inputConsumer;
	protected final List<AbstractOutputProducer<?>> outputProducers = new ArrayList<>();
	private int suspendedProducersCount;

	protected StreamDataReceiver<?>[] dataReceivers = new StreamDataReceiver[0];

	protected abstract class AbstractInputConsumer extends AbstractStreamConsumer<I> {
		protected StreamDataReceiver<?>[] dataReceivers;

		public AbstractInputConsumer() {
			super(AbstractStreamTransformer_1_N.this.eventloop);
		}

		@Override
		protected final void onStarted() {
			onUpstreamStarted();
		}

		protected void onUpstreamStarted() {
		}

		@Override
		protected final void onEndOfStream() {
			onUpstreamEndOfStream();
		}

		protected abstract void onUpstreamEndOfStream();

		@Override
		protected void onError(Exception e) {
			for (AbstractOutputProducer<?> downstreamProducer : outputProducers) {
				downstreamProducer.closeWithError(e);
			}
		}
	}

	protected abstract class AbstractOutputProducer<O> extends AbstractStreamProducer<O> {
		protected int index;

		public AbstractOutputProducer() {
			super(AbstractStreamTransformer_1_N.this.eventloop);
		}

		@Override
		protected final void onError(Exception e) {
			for (AbstractOutputProducer<?> downstreamProducer : outputProducers) {
				if (downstreamProducer != this) {
					downstreamProducer.closeWithError(e);
				}
			}
			inputConsumer.closeWithError(e);
		}

		@Override
		protected final void onDataReceiverChanged() {
			inputConsumer.dataReceivers[index] = downstreamDataReceiver;
		}

		@Override
		protected final void onSuspended() {
			suspendedProducersCount++;
			onDownstreamSuspended();
		}

		protected abstract void onDownstreamSuspended();

		@Override
		protected final void onResumed() {
			suspendedProducersCount--;
			onDownstreamResumed();
		}

		protected abstract void onDownstreamResumed();
	}

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this consumer will run
	 */
	public AbstractStreamTransformer_1_N(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected void setInputConsumer(AbstractInputConsumer inputConsumer) {
		this.inputConsumer = inputConsumer;
		this.inputConsumer.dataReceivers = dataReceivers;
	}

	protected <T, P extends AbstractOutputProducer<T>> P addOutput(final P downstreamProducer) {
		StreamDataReceiver<?>[] oldDataReceivers = dataReceivers;
		StreamDataReceiver<?>[] newDataReceivers = new StreamDataReceiver[oldDataReceivers.length + 1];
		arraycopy(oldDataReceivers, 0, newDataReceivers, 0, oldDataReceivers.length);
		dataReceivers = newDataReceivers;
		if (inputConsumer != null) {
			inputConsumer.dataReceivers = newDataReceivers;
		}

		downstreamProducer.index = outputProducers.size();
		outputProducers.add(downstreamProducer);
		return downstreamProducer;
	}

	/**
	 * Checks if all producers of this consumer are ready
	 *
	 * @return true if all are ready, false else
	 */
	protected boolean allOutputsResumed() {
		return suspendedProducersCount == 0;
	}

	protected void sendEndOfStreamToDownstreams() {
		for (AbstractOutputProducer<?> downstreamProducer : outputProducers) {
			downstreamProducer.sendEndOfStream();
		}
	}

	protected void closeWithError(Exception e) {
		inputConsumer.closeWithError(e);
		for (AbstractOutputProducer<?> downstreamProducer : outputProducers) {
			downstreamProducer.closeWithError(e);
		}
	}

	public StreamConsumer<I> getInput() {
		return inputConsumer;
	}

	@Override
	public List<? extends StreamProducer<?>> getOutputs() {
		return unmodifiableList(outputProducers);
	}

	@Override
	public StreamProducer<?> getOutput(int index) {
		return outputProducers.get(index);
	}
}
