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

public abstract class StreamProducerDecorator<T> extends AbstractStreamProducer<T> {

	protected StreamProducer<T> decoratedProducer;
	protected InternalConsumer internalConsumer;

	private class InternalConsumer extends AbstractStreamConsumer<T> {
		protected InternalConsumer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public StreamDataReceiver<T> getDataReceiver() {
			return StreamProducerDecorator.this.getDataReceiver();
		}

		@Override
		public void onEndOfStream() {
			StreamProducerDecorator.this.onEndOfStream();
		}

		@Override
		public void onError(Exception e) {
			StreamProducerDecorator.this.onError(e);
		}
	}

	public StreamProducerDecorator(Eventloop eventloop) {
		super(eventloop);
	}

	public StreamProducerDecorator(Eventloop eventloop, StreamProducer<T> decorateProducer) {
		this(eventloop);
		decorate(decorateProducer);
	}

	public void decorate(StreamProducer<T> decoratedProducer) {
		checkState(this.decoratedProducer == null, "Already decorated: %s, new: %s", this.decoratedProducer, decoratedProducer);
		this.decoratedProducer = checkNotNull(decoratedProducer);
		this.internalConsumer = new InternalConsumer(eventloop);
		this.decoratedProducer.streamTo(internalConsumer);
	}

	@Override
	public void bindDataReceiver() {
		super.bindDataReceiver();
		if (decoratedProducer != null) {
			decoratedProducer.bindDataReceiver();
		}
	}

	protected StreamDataReceiver<T> getDataReceiver() {
		return getDownstreamDataReceiver();
	}

	protected void onEndOfStream() {
		sendEndOfStream();
	}

	protected void onError(Exception e) {
		sendError(e);
	}

	@Override
	protected void onSuspended() {
		decoratedProducer.suspend();
	}

	@Override
	protected void onResumed() {
		decoratedProducer.resume();
	}

	@Override
	protected void onClosed() {
		decoratedProducer.close();
	}

	@Override
	protected void onClosedWithError(Exception e) {
		decoratedProducer.closeWithError(e);
	}
}
