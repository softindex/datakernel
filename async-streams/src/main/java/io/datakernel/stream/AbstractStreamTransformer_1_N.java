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
	protected final Eventloop eventloop;

	protected AbstractUpstreamConsumer upstreamConsumer;
	protected final List<AbstractDownstreamProducer<?>> downstreamProducers = new ArrayList<>();
	private int suspendedProducersCount;

	protected abstract class AbstractUpstreamConsumer extends AbstractStreamConsumer<I> {
		public AbstractUpstreamConsumer() {
			super(AbstractStreamTransformer_1_N.this.eventloop);
		}

		@Override
		protected final void onStarted() {
			onUpstreamStarted();
		}

		protected abstract void onUpstreamStarted();

		@Override
		protected final void onEndOfStream() {
			onUpstreamEndOfStream();
		}

		protected abstract void onUpstreamEndOfStream();

		@Override
		protected void onError(Exception e) {
			for (AbstractStreamProducer<?> downstreamProducer : downstreamProducers) {
				downstreamProducer.closeWithError(e);
			}
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
		public final void close() {
			super.close();
		}

		@Override
		public final void closeWithError(Exception e) {
			super.closeWithError(e);
		}
	}

	protected abstract class AbstractDownstreamProducer<O> extends AbstractStreamProducer<O> {
		protected int index;

		public AbstractDownstreamProducer() {
			super(AbstractStreamTransformer_1_N.this.eventloop);
		}

		@Override
		protected final void onError(Exception e) {
			for (AbstractStreamProducer<?> downstreamProducer : downstreamProducers) {
				if (downstreamProducer != this) {
					downstreamProducer.closeWithError(e);
				}
			}
			upstreamConsumer.closeWithError(e);
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

		@Override
		public void produce() {
			super.produce();
		}

		@Override
		public void send(O item) {
			super.send(item);
		}

		@Override
		public void sendEndOfStream() {
			super.sendEndOfStream();
		}

	}

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this consumer will run
	 */
	public AbstractStreamTransformer_1_N(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	protected void setUpstreamConsumer(AbstractUpstreamConsumer upstreamConsumer) {
		this.upstreamConsumer = upstreamConsumer;
	}

	protected <T> StreamProducer<T> addOutput(final AbstractDownstreamProducer<T> downstreamProducer) {
		downstreamProducer.index = downstreamProducers.size();
		downstreamProducers.add(downstreamProducer);
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
		for (AbstractDownstreamProducer<?> downstreamProducer : downstreamProducers) {
			downstreamProducer.sendEndOfStream();
		}
	}

	protected void closeWithError(Exception e) {
		upstreamConsumer.closeWithError(e);
		for (AbstractDownstreamProducer<?> downstreamProducer : downstreamProducers) {
			downstreamProducer.closeWithError(e);
		}
	}

	// consumer

	@Override
	public final StreamDataReceiver<I> getDataReceiver() {
		return upstreamConsumer.getDataReceiver();
	}

	@Override
	public final void setUpstream(StreamProducer<I> upstreamProducer) {
		upstreamConsumer.setUpstream(upstreamProducer);
	}

	@Override
	public final StreamProducer<I> getUpstream() {
		return upstreamConsumer.getUpstream();
	}

	@Override
	public final void onProducerEndOfStream() {
		upstreamConsumer.onProducerEndOfStream();
	}

	@Override
	public final void onProducerError(Exception e) {
		upstreamConsumer.onProducerError(e);
	}

	@Override
	public final void addConsumerCompletionCallback(CompletionCallback completionCallback) {
		upstreamConsumer.addConsumerCompletionCallback(completionCallback);
	}

}
