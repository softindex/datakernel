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

public class StreamProducerSwitcher<T> extends AbstractStreamProducer<T> {

	private InternalConsumer currentInternalConsumer;

	private class InternalConsumer extends AbstractStreamConsumer<T> {
		protected InternalConsumer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			if (this != currentInternalConsumer || status >= END_OF_STREAM) {
				return new StreamDataReceiver<T>() {
					@Override
					public void onData(T item) {
					}
				};
			}
			return downstreamDataReceiver;
		}

		@Override
		public void onProducerEndOfStream() {
			if (this == currentInternalConsumer) {
				sendEndOfStream();
				close();
			}
		}

		@Override
		public void onProducerError(Exception e) {
			if (this == currentInternalConsumer) {
//				upstreamProducer.onConsumerError(e);
//				onConsumerError(e);
				downstreamConsumer.onProducerError(e);
				closeWithError(e);
			}
		}
	}

	public StreamProducerSwitcher(Eventloop eventloop) {
		this(eventloop, new StreamProducers.Idle<T>(eventloop));
	}

	public StreamProducerSwitcher(Eventloop eventloop, StreamProducer<T> initialProducer) {
		super(eventloop);
		switchProducerTo(initialProducer);
	}

	public void switchProducerTo(StreamProducer<T> newUpstreamProducer) {
		InternalConsumer prevProducer = currentInternalConsumer;
		currentInternalConsumer = new InternalConsumer(eventloop);
		newUpstreamProducer.streamTo(currentInternalConsumer);
		if (prevProducer != null && prevProducer.upstreamProducer != null) {
			prevProducer.upstreamProducer.bindDataReceiver();
//			prevProducer.closeUpstream();
		}
		if (status == END_OF_STREAM || status == CLOSED) {
//			currentInternalConsumer.closeUpstream();
			currentInternalConsumer.onProducerEndOfStream();
		}
		if (status == CLOSED_WITH_ERROR) {
//			currentInternalConsumer.closeUpstreamWithError(getError());
			currentInternalConsumer.onProducerError(getError());
		}
	}

	public StreamProducer<T> getCurrentProducer() {
		return currentInternalConsumer == null ? null : currentInternalConsumer.getUpstream();
	}

	@Override
	public void bindDataReceiver() {
		super.bindDataReceiver();
		if (currentInternalConsumer != null && currentInternalConsumer.getUpstream() != null) {
			currentInternalConsumer.getUpstream().bindDataReceiver();
		}
	}

	@Override
	protected void onSuspended() {
		currentInternalConsumer.suspendUpstream();
	}

	@Override
	protected void onResumed() {
		currentInternalConsumer.resumeUpstream();
	}

	@Override
	protected void onClosed() {
//		currentInternalConsumer.closeUpstream();
	}

	@Override
	protected void onClosedWithError(Exception e) {
		downstreamConsumer.onProducerError(e);
		if (currentInternalConsumer != null) {
			currentInternalConsumer.onProducerError(e);
		}
	}
}
