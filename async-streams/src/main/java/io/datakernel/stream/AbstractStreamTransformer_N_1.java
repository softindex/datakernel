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
public abstract class AbstractStreamTransformer_N_1<O> implements StreamProducer<O> {
	protected final Eventloop eventloop;

	protected final List<AbstractUpstreamConsumer<?>> upstreamConsumers = new ArrayList<>();
	protected AbstractDownstreamProducer downstreamProducer;

	protected int countEndOfStreams = 0;

	protected abstract class AbstractUpstreamConsumer<I> extends AbstractStreamConsumer<I> {
		protected int index;

		public AbstractUpstreamConsumer() {
			super(AbstractStreamTransformer_N_1.this.eventloop);
		}

		@Override
		protected final void onStarted() {
			onUpstreamStarted();
		}

		protected abstract void onUpstreamStarted();

		@Override
		protected final void onEndOfStream() {
			countEndOfStreams++;
			onUpstreamEndOfStream();
		}

		protected abstract void onUpstreamEndOfStream();

		@Override
		protected final void onError(Exception e) {
			countEndOfStreams++;
			for (AbstractUpstreamConsumer<?> input : upstreamConsumers) {
				if (input != this) {
					input.closeWithError(e);
				}
			}
			downstreamProducer.closeWithError(e);
		}

		@Override
		public final void suspend() {
			super.suspend();
		}

		@Override
		public final void resume() {
			super.resume();
		}

		@Override
		public final void closeWithError(Exception e) {
			super.closeWithError(e);
		}
	}

	protected abstract class AbstractDownstreamProducer extends AbstractStreamProducer<O> {
		public AbstractDownstreamProducer() {
			super(AbstractStreamTransformer_N_1.this.eventloop);
		}

		@Override
		protected final void onDataReceiverChanged() {
			for (AbstractUpstreamConsumer<?> input : upstreamConsumers) {
				if (input.getUpstream() != null) {
					input.getUpstream().bindDataReceiver();
				}
			}
		}

		@Override
		protected final void onStarted() {
			for (AbstractUpstreamConsumer<?> input : upstreamConsumers) {
				// TODO (vsavchuk) input.getUpstream() == null?
				if (input.getUpstream() != null) {
					input.getUpstream().bindDataReceiver();
				}
			}
			onDownstreamStarted();
		}

		protected abstract void onDownstreamStarted();

		@Override
		protected final void onError(Exception e) {
			for (AbstractUpstreamConsumer<?> input : upstreamConsumers) {
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

		@Override
		public final void produce() {
			super.produce();
		}

		@Override
		public final void send(O item) {
			super.send(item);
		}

		@Override
		public final void sendEndOfStream() {
			super.sendEndOfStream();
		}
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
	protected <T> StreamConsumer<T> addInput(final AbstractUpstreamConsumer<T> upstreamConsumer) {
		checkNotNull(upstreamConsumer);
		upstreamConsumer.index = upstreamConsumers.size();
		upstreamConsumers.add(upstreamConsumer);
		return upstreamConsumer;
	}

	protected final void suspendAllUpstreams() {
		for (AbstractUpstreamConsumer<?> upstreamConsumer : upstreamConsumers) {
			upstreamConsumer.suspend();
		}
	}

	protected final void resumeAllUpstreams() {
		for (AbstractUpstreamConsumer<?> upstreamConsumer : upstreamConsumers) {
			upstreamConsumer.resume();
		}
	}

	protected boolean allUpstreamsEndOfStream() {
		return countEndOfStreams == upstreamConsumers.size();
	}

	// StreamConsumer interface

	@Override
	public final void bindDataReceiver() {
		downstreamProducer.bindDataReceiver();
	}

	@Override
	public final void streamTo(StreamConsumer<O> downstreamConsumer) {
		downstreamProducer.streamTo(downstreamConsumer);
	}

	@Override
	public final void onConsumerSuspended() {
		downstreamProducer.onConsumerSuspended();
	}

	@Override
	public final void onConsumerResumed() {
		downstreamProducer.onConsumerResumed();
	}

	@Override
	public final void onConsumerError(Exception e) {
		downstreamProducer.onConsumerError(e);
	}

	@Override
	public StreamStatus getProducerStatus() {
		return downstreamProducer.getProducerStatus();
	}

	public final StreamProducer getDownstreamProducer() {
		return downstreamProducer;
	}

	public final List<? extends StreamConsumer<?>> getUpstreamConsumers() {
		return unmodifiableList(upstreamConsumers);
	}

}
