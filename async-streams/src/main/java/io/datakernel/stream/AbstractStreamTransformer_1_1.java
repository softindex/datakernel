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
import io.datakernel.stream.processor.StreamTransformer;

import static com.google.common.base.Preconditions.checkState;

/**
 * Represent {@link StreamProducer} and {@link StreamConsumer} in the one object.
 * This object can receive and send streams of data.
 *
 * @param <I> type of input data for consumer
 * @param <O> type of output data of producer
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_1_1<I, O> implements StreamTransformer<I, O> {
	protected final Eventloop eventloop;

	private AbstractUpstreamConsumer upstreamConsumer;
	private AbstractDownstreamProducer downstreamProducer;

	protected Object tag;

	protected abstract class AbstractUpstreamConsumer extends AbstractStreamConsumer<I> {
		public AbstractUpstreamConsumer() {
			super(AbstractStreamTransformer_1_1.this.eventloop);
			checkState(upstreamConsumer == null);
			upstreamConsumer = this;
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
		public final void close() {
			super.close();
		}

		@Override
		public void closeWithError(Exception e) {
			super.closeWithError(e);
		}

	}

	protected abstract class AbstractDownstreamProducer extends AbstractStreamProducer<O> {
		public AbstractDownstreamProducer() {
			super(AbstractStreamTransformer_1_1.this.eventloop);
			checkState(downstreamProducer == null);
			downstreamProducer = this;
		}

		@Override
		protected final void onDataReceiverChanged() {
			if (upstreamConsumer.getUpstream() != null) {
				upstreamConsumer.getUpstream().bindDataReceiver();
			}
		}

		@Override
		protected final void onStarted() {
			upstreamConsumer.bindUpstream();
			onDownstreamStarted();
		}

		protected abstract void onDownstreamStarted();

		@Override
		protected void onEndOfStream() {
			upstreamConsumer.close();
		}

		@Override
		protected final void onError(Exception e) {
			upstreamConsumer.closeWithError(e);
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

	protected AbstractStreamTransformer_1_1(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	// upstream

	@Override
	public final StreamDataReceiver<I> getDataReceiver() {
		return upstreamConsumer.getDataReceiver();
	}

	@Override
	public final void onProducerEndOfStream() {
		upstreamConsumer.onProducerEndOfStream();
	}

	@Override
	public final void addConsumerCompletionCallback(CompletionCallback completionCallback) {
		upstreamConsumer.addConsumerCompletionCallback(completionCallback);
	}

	@Override
	public final void onProducerError(Exception e) {
		upstreamConsumer.onProducerError(e);
		downstreamProducer.closeWithError(e);
	}

	@Override
	public final void streamFrom(StreamProducer<I> upstreamProducer) {
		upstreamConsumer.streamFrom(upstreamProducer);
	}

	// downstream

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
	public final void addProducerCompletionCallback(CompletionCallback completionCallback) {
		downstreamProducer.addProducerCompletionCallback(completionCallback);
	}

	@Override
	public final void bindDataReceiver() {
		downstreamProducer.bindDataReceiver();
	}

	protected void suspendUpstream() {
		upstreamConsumer.suspend();
	}

	protected void resumeUpstream() {
		upstreamConsumer.resume();
	}

	public AbstractUpstreamConsumer getUpstreamConsumer() {
		return upstreamConsumer;
	}

	public AbstractDownstreamProducer getDownstreamProducer() {
		return downstreamProducer;
	}

	// misc

	//for test only
	AbstractStreamConsumer.StreamConsumerStatus getUpstreamConsumerStatus() {
		return upstreamConsumer.getStatus();
	}

	// for test only
	AbstractStreamProducer.StreamProducerStatus getDownstreamProducerStatus() {
		return downstreamProducer.getStatus();
	}

	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

}
