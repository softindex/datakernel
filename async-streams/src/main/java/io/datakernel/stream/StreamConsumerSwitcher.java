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

public class StreamConsumerSwitcher<T> extends AbstractStreamConsumer<T> {
	private InternalProducer currentInternalProducer;

	private class InternalProducer extends AbstractStreamProducer<T> {
		protected InternalProducer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public void bindDataReceiver() {
			super.bindDataReceiver();
			if (getUpstream() != null) {
				getUpstream().bindDataReceiver();
			}
		}

		@Override
		public void onSuspended() {
			if (this == currentInternalProducer) {
				suspendUpstream();
			}
		}

		@Override
		public void onResumed() {
			if (this == currentInternalProducer) {
				resumeUpstream();
			}
		}

		@Override
		public void onClosed() {
			if (this == currentInternalProducer) {
				closeUpstream();
			}
		}

		@Override
		protected void onClosedWithError(Exception e) {
			if (this == currentInternalProducer) {
				StreamConsumerSwitcher.this.onError(e);
				downstreamConsumer.onError(e);
			}
		}
	}

	public StreamConsumerSwitcher(Eventloop eventloop) {
		super(eventloop);
	}

	public StreamConsumerSwitcher(Eventloop eventloop, StreamConsumer<T> initialConsumer) {
		this(eventloop);
		switchConsumerTo(initialConsumer);
	}

	public void switchConsumerTo(StreamConsumer<T> newDownstreamConsumer) {
		final InternalProducer prevProducer = this.currentInternalProducer;
		if (prevProducer != null) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					prevProducer.close();
				}
			});
		}
		currentInternalProducer = new InternalProducer(eventloop);
		currentInternalProducer.streamTo(newDownstreamConsumer);
	}

	public StreamConsumer<T> getCurrentConsumer() {
		return currentInternalProducer == null ? null : currentInternalProducer.getDownstream();
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return currentInternalProducer != null ? currentInternalProducer.getDownstreamDataReceiver() : null;
	}

	@Override
	public void onEndOfStream() {
		currentInternalProducer.sendEndOfStream();
	}

	@Override
	public void onError(Exception e) {
		upstreamProducer.closeWithError(e);
		if (currentInternalProducer != null) {
			currentInternalProducer.closeWithError(e);
		}
	}

}
