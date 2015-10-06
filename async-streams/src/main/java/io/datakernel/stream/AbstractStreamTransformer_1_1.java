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

	protected AbstractUpstreamConsumer upstreamConsumer;
	protected AbstractDownstreamProducer downstreamProducer;
	protected StreamDataReceiver<O> downstreamDataReceiver;

	protected Object tag;

	protected abstract class AbstractUpstreamConsumer extends AbstractStreamConsumer<I> {
		protected int index;

		public AbstractUpstreamConsumer() {
			super(AbstractStreamTransformer_1_1.this.eventloop);
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
		public void suspend() {
			super.suspend();
		}

		@Override
		public void resume() {
			super.resume();
		}

		@Override
		public void close() {
			super.close();
		}

		@Override
		public void closeWithError(Exception e) {
			super.closeWithError(e);
		}
	}

	protected abstract class AbstractDownstreamProducer extends AbstractStreamProducer<O> {

		protected int index;

		public AbstractDownstreamProducer() {
			super(AbstractStreamTransformer_1_1.this.eventloop);
		}

		@Override
		protected final void onDataReceiverChanged() {
			AbstractStreamTransformer_1_1.this.downstreamDataReceiver = this.downstreamDataReceiver;
			if (upstreamConsumer.getUpstream() != null) {
				upstreamConsumer.getUpstream().bindDataReceiver();
			}
		}

		@Override
		protected final void onStarted() {
			if (upstreamConsumer.getUpstream() != null) {
				upstreamConsumer.getUpstream().bindDataReceiver();
			}
			onDownstreamStarted();
		}

		protected abstract void onDownstreamStarted();

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

	protected void closeWithError(Exception e) {
		downstreamProducer.closeWithError(e);
		upstreamConsumer.closeWithError(e);
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
		if (upstreamConsumer.getUpstream() != null) {
			upstreamConsumer.getUpstream().bindDataReceiver();
		}
	}

	public AbstractUpstreamConsumer getUpstreamConsumer() {
		return upstreamConsumer;
	}

	public AbstractDownstreamProducer getDownstreamProducer() {
		return downstreamProducer;
	}

	// misc

	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

}
