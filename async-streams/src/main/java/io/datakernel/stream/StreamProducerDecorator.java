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

/**
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public class StreamProducerDecorator<T> implements StreamProducer<T> {
	protected final Eventloop eventloop;
	private final AbstractStreamConsumer<T> upstreamConsumer;
	private final AbstractStreamProducer<T> downstreamProducer;

	public StreamProducerDecorator(Eventloop eventloop, StreamProducer<T> actualProducer) {
		this(eventloop);
		setActualProducer(actualProducer);
	}

	public StreamProducerDecorator(Eventloop eventloop) {
		this.eventloop = eventloop;
		this.upstreamConsumer = new AbstractStreamConsumer<T>(eventloop) {
			@Override
			protected void onStarted() {
			}

			@Override
			protected void onEndOfStream() {
				downstreamProducer.sendEndOfStream();
			}

			@Override
			protected void onError(Exception e) {
				downstreamProducer.closeWithError(e);
			}

			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return downstreamProducer.getDownstreamDataReceiver();
			}
		};
		this.downstreamProducer = new AbstractStreamProducer<T>(eventloop) {
			@Override
			protected void onStarted() {
				StreamProducerDecorator.this.onStarted();
			}

			@Override
			protected void onDataReceiverChanged() {
				upstreamConsumer.bindUpstream();
			}

			@Override
			protected void onSuspended() {
				upstreamConsumer.suspend();
			}

			@Override
			protected void onResumed() {
				upstreamConsumer.resume();
			}

			@Override
			protected void onError(Exception e) {
				upstreamConsumer.closeWithError(e);
			}
		};
	}

	public final void setActualProducer(StreamProducer<T> actualProducer) {
		actualProducer.streamTo(new StreamConsumer<T>() {
			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return upstreamConsumer.getDataReceiver();
			}

			@Override
			public void streamFrom(StreamProducer<T> upstreamProducer) {
				upstreamConsumer.streamFrom(upstreamProducer);
			}

			@Override
			public void onProducerEndOfStream() {
				StreamProducerDecorator.this.onProducerEndOfStream();
			}

			@Override
			public void onProducerError(Exception e) {
				StreamProducerDecorator.this.onProducerError(e);
			}

			@Override
			public StreamStatus getConsumerStatus() {
				return upstreamConsumer.getConsumerStatus();
			}
		});
	}

	@Override
	public final void streamTo(StreamConsumer<T> downstreamConsumer) {
		downstreamProducer.streamTo(downstreamConsumer);
	}

	@Override
	public final void bindDataReceiver() {
		downstreamProducer.bindDataReceiver();
	}

	public void sendEndOfStream() {
		downstreamProducer.sendEndOfStream();
	}

	public void closeWithError(Exception e) {
		upstreamConsumer.closeWithError(e);
		downstreamProducer.closeWithError(e);
	}

	// extension hooks, intended for override:

	protected void onStarted() {
	}

	protected void onProducerEndOfStream() {
		upstreamConsumer.onProducerEndOfStream();
	}

	protected void onProducerError(Exception e) {
		upstreamConsumer.onProducerError(e);
	}

	@Override
	public void onConsumerSuspended() {
		downstreamProducer.onConsumerSuspended();
	}

	@Override
	public void onConsumerResumed() {
		downstreamProducer.onConsumerResumed();
	}

	@Override
	public void onConsumerError(Exception e) {
		downstreamProducer.onConsumerError(e);
	}

	@Override
	public final StreamStatus getProducerStatus() {
		return downstreamProducer.getProducerStatus();
	}
}