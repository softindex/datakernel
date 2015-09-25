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

/**
 * Represents  {@link AbstractStreamTransformer_1_1} with open consumer and producer statuses
 *
 * @param <I> type of input data for consumer
 * @param <O> type of output data of producer
 */
public abstract class AbstractStreamTransformer_1_1_Stateless<I, O> extends AbstractStreamTransformer_1_1<I, O> {

	protected final class UpstreamConsumer extends AbstractUpstreamConsumer {
		@Override
		protected void onUpstreamStarted() {
		}

		@Override
		protected void onUpstreamEndOfStream() {
			AbstractStreamTransformer_1_1_Stateless.this.onUpstreamEndOfStream();
		}

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return AbstractStreamTransformer_1_1_Stateless.this.getUpstreamDataReceiver();
		}
	}

	protected final class DownstreamProducer extends AbstractDownstreamProducer {
		@Override
		protected void onDownstreamStarted() {
		}

		@Override
		protected void onDownstreamSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			upstreamConsumer.resume();
		}
	}

	protected AbstractStreamTransformer_1_1_Stateless(Eventloop eventloop) {
		super(eventloop);
		this.upstreamConsumer = new UpstreamConsumer();
		this.downstreamProducer = new DownstreamProducer();
		// TODO (vsavchuk) ???
		this.downstreamDataReceiver = downstreamProducer.getDownstreamDataReceiver();
	}

	protected abstract StreamDataReceiver<I> getUpstreamDataReceiver();

	protected abstract void onUpstreamEndOfStream();
}
