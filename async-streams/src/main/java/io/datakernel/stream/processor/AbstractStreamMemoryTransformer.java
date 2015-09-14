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

package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.AbstractStreamTransformer_M_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;

/**
 * Represents the producer with few consumers which have a state for storing intermediate results.
 *
 * @param <I> type of input data
 * @param <S> type of state
 * @param <O> type of output data
 */
public abstract class AbstractStreamMemoryTransformer<I, S, O> extends AbstractStreamTransformer_M_1<O> implements StreamDataReceiver<I> {
	protected S state;

	protected AbstractStreamMemoryTransformer(Eventloop eventloop) {
		super(eventloop);
		this.state = newState();
	}

	protected abstract S newState();

	/**
	 * Method which is called for each item when data is receiving
	 *
	 * @param state state of this instance
	 * @param item  received item
	 */
	protected abstract void apply(S state, I item);

	/**
	 * When stream ends this method can use states for searching result
	 *
	 * @param state state of this object
	 */
	protected abstract void afterEndOfStream(S state);

	/**
	 * Adds a new consumer to this instance
	 *
	 * @return new consumer
	 */
	public final StreamConsumer<I> newInput() {
		return addInput(new Input(eventloop));
	}

	private final class Input extends AbstractStreamConsumer<I> {
		public Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return AbstractStreamMemoryTransformer.this;
		}

		@Override
		public void onProducerEndOfStream() {
			if (!allUpstreamsEndOfStream())
				return;

			afterEndOfStream(state);
			state = null;

			produce();
		}

		@Override
		public void onProducerError(Exception e) {
			upstreamProducer.onConsumerError(e);
			onConsumerError(e);
		}
	}

	/**
	 * This method is called if consumer was changed for changing consumer status
	 * and its dependencies
	 */
	@Override
	protected void onProducerStarted() {
		if (inputs.isEmpty()) {
			sendEndOfStream();
		}
	}

	/**
	 * Action which will take place after changing status to suspend
	 */
	@Override
	protected void onSuspended() {
	}

	/**
	 * Action which will take place after changing status to resume
	 */
	@Override
	protected void onResumed() {
		if (state == null) {
			resumeProduce();
		}
	}

	/**
	 * On receiving data it calls function apply which must be defined
	 *
	 * @param item receiving item
	 */
	@Override
	public void onData(I item) {
		apply(state, item);
	}

}
