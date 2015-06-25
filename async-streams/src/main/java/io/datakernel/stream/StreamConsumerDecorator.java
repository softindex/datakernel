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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class StreamConsumerDecorator<T> extends AbstractStreamConsumer<T> {

	protected StreamConsumer<T> decoratedConsumer;
	protected InternalProducer internalProducer;

	private class InternalProducer extends AbstractStreamProducer<T> {
		protected InternalProducer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onSuspended() {
			StreamConsumerDecorator.this.onSuspended();
		}

		@Override
		protected void onResumed() {
			StreamConsumerDecorator.this.onResumed();
		}

		@Override
		protected void onClosed() {
			StreamConsumerDecorator.this.onClosed();
		}

		@Override
		protected void onClosedWithError(Exception e) {
			StreamConsumerDecorator.this.onClosedWithError(e);
		}
	}

	protected StreamConsumerDecorator(Eventloop eventloop) {
		super(eventloop);
	}

	public StreamConsumerDecorator(Eventloop eventloop, StreamConsumer<T> decoratedConsumer) {
		this(eventloop);
		decorate(decoratedConsumer);
	}

	public void decorate(StreamConsumer<T> decoratedConsumer) {
		checkState(this.decoratedConsumer == null, "Already decorated: %s, new: %s", this.decoratedConsumer, decoratedConsumer);
		this.decoratedConsumer = checkNotNull(decoratedConsumer);
		this.internalProducer = new InternalProducer(eventloop);
		this.internalProducer.streamTo(this.decoratedConsumer);
	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return decoratedConsumer.getDataReceiver();
	}

	protected void onSuspended() {
		suspendUpstream();
	}

	protected void onResumed() {
		resumeUpstream();
	}

	protected void onClosed() {
		closeUpstream();
	}

	protected void onClosedWithError(Exception e) {
		closeUpstreamWithError(e);
	}

	@Override
	public void onEndOfStream() {
		decoratedConsumer.onEndOfStream();
	}

	@Override
	public void onError(Exception e) {
		decoratedConsumer.onError(e);
	}

}
