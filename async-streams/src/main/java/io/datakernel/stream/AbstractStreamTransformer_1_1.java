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

		protected void onUpstreamStarted() {

		}

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

		protected void onDownstreamStarted() {

		}

		@Override
		protected void onError(Exception e) {
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

	@Override
	public StreamConsumer<I> getInput() {
		return upstreamConsumer;
	}

	@Override
	public StreamProducer<O> getOutput() {
		return downstreamProducer;
	}

	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

}
