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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;

/**
 * Represents {@link AbstractStreamProducer} with few {@link AbstractStreamConsumer}.
 *
 * @param <O> type of sending items
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_N_1<O> implements HasOutput<O>, HasInputs {
	protected final Eventloop eventloop;

	protected final List<AbstractInputConsumer<?>> inputConsumers = new ArrayList<>();
	protected AbstractOutputProducer outputProducer;

	protected int countEndOfStreams = 0;

	protected abstract class AbstractInputConsumer<I> extends AbstractStreamConsumer<I> {
		protected int index;

		public AbstractInputConsumer() {
			super(AbstractStreamTransformer_N_1.this.eventloop);
		}

		@Override
		protected final void onStarted() {
			onUpstreamStarted();
		}

		protected void onUpstreamStarted() {

		}

		@Override
		protected final void onEndOfStream() {
			countEndOfStreams++;
			onUpstreamEndOfStream();
		}

		protected abstract void onUpstreamEndOfStream();

		@Override
		protected final void onError(Exception e) {
			countEndOfStreams++;
			for (AbstractInputConsumer<?> input : inputConsumers) {
				if (input != this) {
					input.closeWithError(e);
				}
			}
			outputProducer.closeWithError(e);
		}
	}

	protected abstract class AbstractOutputProducer extends AbstractStreamProducer<O> {
		public AbstractOutputProducer() {
			super(AbstractStreamTransformer_N_1.this.eventloop);
		}

		@Override
		protected final void onDataReceiverChanged() {
			for (AbstractInputConsumer<?> input : inputConsumers) {
				if (input.getUpstream() != null) {
					input.getUpstream().bindDataReceiver();
				}
			}
		}

		@Override
		protected final void onStarted() {
			for (AbstractInputConsumer<?> input : inputConsumers) {
				if (input.getUpstream() != null) {
					input.getUpstream().bindDataReceiver();
				}
			}
			onDownstreamStarted();
		}

		protected void onDownstreamStarted() {
		}

		@Override
		protected final void onError(Exception e) {
			for (AbstractInputConsumer<?> input : inputConsumers) {
				input.closeWithError(e);
			}
		}

		@Override
		protected final void onSuspended() {
			onDownstreamSuspended();
		}

		protected abstract void onDownstreamSuspended();

		@Override
		protected final void onResumed() {
			onDownstreamResumed();
		}

		protected abstract void onDownstreamResumed();
	}

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this producer will run
	 */
	public AbstractStreamTransformer_N_1(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	/**
	 * Adds a new stream consumer to this producer
	 *
	 * @param <T>              type of stream consumer
	 * @param upstreamConsumer stream consumer events handler
	 */
	protected <T> StreamConsumer<T> addInput(final AbstractInputConsumer<T> upstreamConsumer) {
		checkNotNull(upstreamConsumer);
		upstreamConsumer.index = inputConsumers.size();
		inputConsumers.add(upstreamConsumer);
		return upstreamConsumer;
	}

	protected final void suspendAllUpstreams() {
		for (AbstractInputConsumer<?> upstreamConsumer : inputConsumers) {
			upstreamConsumer.suspend();
		}
	}

	protected final void resumeAllUpstreams() {
		for (AbstractInputConsumer<?> upstreamConsumer : inputConsumers) {
			upstreamConsumer.resume();
		}
	}

	protected boolean allUpstreamsEndOfStream() {
		return countEndOfStreams == inputConsumers.size();
	}

	@Override
	public StreamProducer<O> getOutput() {
		return outputProducer;
	}

	@Override
	public final List<? extends StreamConsumer<?>> getInputs() {
		return unmodifiableList(inputConsumers);
	}

	@Override
	public StreamConsumer<?> getInput(int index) {
		return inputConsumers.get(index);
	}
}
