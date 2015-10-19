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
 * See also:
 * <br><a href="http://en.wikipedia.org/wiki/Decorator_pattern">Decorator Pattern</a>
 * <br>{@link com.google.common.collect.ForwardingObject}
 * <br>{@link com.google.common.collect.ForwardingCollection}
 *
 * @param <T> item type
 */
public class StreamConsumerDecorator<T> implements StreamConsumer<T> {
	protected final Eventloop eventloop;
	protected final AbstractStreamConsumer<T> upstreamConsumer;
	protected final AbstractStreamProducer<T> downstreamProducer;

	public StreamConsumerDecorator(Eventloop eventloop, final StreamConsumer<T> actualConsumer) {
		this(eventloop);
		setActualConsumer(actualConsumer);
	}

	public StreamConsumerDecorator(Eventloop eventloop) {
		this.eventloop = eventloop;
		this.upstreamConsumer = new AbstractStreamConsumer<T>(eventloop) {
			@Override
			protected void onStarted() {
				StreamConsumerDecorator.this.onStarted();
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

	public final void setActualConsumer(StreamConsumer<T> actualConsumer) {
		new StreamProducer<T>() {
			@Override
			public void streamTo(StreamConsumer<T> downstreamConsumer) {
				downstreamProducer.streamTo(downstreamConsumer);
			}

			@Override
			public void bindDataReceiver() {
				downstreamProducer.bindDataReceiver();
			}

			@Override
			public void onConsumerSuspended() {
				StreamConsumerDecorator.this.onConsumerSuspended();
			}

			@Override
			public void onConsumerResumed() {
				StreamConsumerDecorator.this.onConsumerResumed();
			}

			@Override
			public void onConsumerError(Exception e) {
				StreamConsumerDecorator.this.onConsumerError(e);
			}

			@Override
			public StreamStatus getProducerStatus() {
				return downstreamProducer.getProducerStatus();
			}

			@Override
			public Exception getProducerException() {
				return downstreamProducer.getProducerException();
			}
		}.streamTo(actualConsumer);
	}

	@Override
	public final StreamDataReceiver<T> getDataReceiver() {
		return upstreamConsumer.getDataReceiver();
	}

	@Override
	public final void streamFrom(StreamProducer<T> upstreamProducer) {
		upstreamConsumer.streamFrom(upstreamProducer);
	}

	// extension hooks, intended for override:

	protected final void onStarted() {
	}

	protected void onConsumerSuspended() {
		downstreamProducer.onConsumerSuspended();
	}

	protected void onConsumerResumed() {
		downstreamProducer.onConsumerResumed();
	}

	protected void onConsumerError(Exception e) {
		downstreamProducer.onConsumerError(e);
	}

	@Override
	public void onProducerEndOfStream() {
		upstreamConsumer.onProducerEndOfStream();
	}

	@Override
	public void onProducerError(Exception e) {
		upstreamConsumer.onProducerError(e);
	}

	@Override
	public final StreamStatus getConsumerStatus() {
		return upstreamConsumer.getConsumerStatus();
	}

	@Override
	public Exception getConsumerException() {
		return upstreamConsumer.getConsumerException();
	}
}
