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
import io.datakernel.stream.processor.StreamTransformer;

public class ErrorIgnoringTransformer<T> implements StreamTransformer<T, T> {
	protected final Eventloop eventloop;

	private UpstreamConsumer upstreamConsumer;
	private DownstreamProducer downstreamProducer;

	protected Object tag;

	private class UpstreamConsumer extends AbstractStreamConsumer<T> {
		public UpstreamConsumer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			downstreamProducer.sendEndOfStream();
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return downstreamProducer.getDownstreamDataReceiver();
		}

		@Override
		protected void onError(Exception e) {
			downstreamProducer.sendEndOfStream();
		}
	}

	private class DownstreamProducer extends AbstractStreamProducer<T> {
		public DownstreamProducer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		protected void onSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onResumed() {
			upstreamConsumer.resume();
		}
	}

	public ErrorIgnoringTransformer(Eventloop eventloop) {
		this.eventloop = eventloop;
		this.upstreamConsumer = new UpstreamConsumer(eventloop);
		this.downstreamProducer = new DownstreamProducer(eventloop);
	}

	// upstream

	@Override
	public final StreamDataReceiver<T> getDataReceiver() {
		return upstreamConsumer.getDataReceiver();
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
	public final void streamFrom(StreamProducer<T> upstreamProducer) {
		upstreamConsumer.streamFrom(upstreamProducer);
	}

	@Override
	public StreamStatus getConsumerStatus() {
		return upstreamConsumer.getConsumerStatus();
	}

	// downstream

	@Override
	public final void streamTo(StreamConsumer<T> downstreamConsumer) {
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
	public final void bindDataReceiver() {
		downstreamProducer.bindDataReceiver();
	}

	@Override
	public StreamStatus getProducerStatus() {
		return downstreamProducer.getProducerStatus();
	}

	@Override
	public Exception getConsumerException() {
		return upstreamConsumer.getConsumerException();
	}

	@Override
	public Exception getProducerException() {
		return downstreamProducer.getProducerException();
	}

	//for test only
	StreamStatus getUpstreamConsumerStatus() {
		return upstreamConsumer.getConsumerStatus();
	}

	// for test only
	StreamStatus getDownstreamProducerStatus() {
		return downstreamProducer.getProducerStatus();
	}

	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}
}
