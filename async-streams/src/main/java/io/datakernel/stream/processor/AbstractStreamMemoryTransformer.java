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
import io.datakernel.stream.AbstractStreamTransformer_N_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;

/**
 * Represents the producer with few consumers which have a state for storing intermediate results.
 *
 * @param <I> type of input data
 * @param <S> type of state
 * @param <O> type of output data
 */
public abstract class AbstractStreamMemoryTransformer<I, S, O> extends AbstractStreamTransformer_N_1<O> implements StreamDataReceiver<I> {
	protected S state;

	private final class DownstreamProducer extends AbstractDownstreamProducer {

		@Override
		protected void onDownstreamSuspended() {

		}

		@Override
		protected void onDownstreamResumed() {
			if (state == null) {
				resumeProduce();
			}
		}

		@Override
		protected void doProduce() {
			AbstractStreamMemoryTransformer.this.downstreamProducerDoProduce();
		}
	}

	protected abstract void downstreamProducerDoProduce();

	private final class UpstreamConsumer extends AbstractUpstreamConsumer<I> {

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return AbstractStreamMemoryTransformer.this;
		}

		@Override
		protected void onUpstreamStarted() {
			if (upstreamConsumers.isEmpty()) {
				downstreamProducer.sendEndOfStream();
			}
		}

		@Override
		protected void onUpstreamEndOfStream() {
			if (!allUpstreamsEndOfStream())
				return;

			afterEndOfStream(state);
			state = null;

			downstreamProducer.produce();
		}
	}

	protected AbstractStreamMemoryTransformer(Eventloop eventloop) {
		super(eventloop);
		this.state = newState();
		this.downstreamProducer = new DownstreamProducer();
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
		return addInput(new UpstreamConsumer());
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
