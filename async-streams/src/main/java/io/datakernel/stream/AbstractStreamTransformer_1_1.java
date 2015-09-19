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
	protected final AbstractStreamConsumer<I> internalConsumer;
	protected final AbstractStreamProducer<O> internalProducer;
	protected final Eventloop eventloop;

	protected Object tag;

	protected AbstractStreamTransformer_1_1(Eventloop eventloop) {
		this.eventloop = eventloop;
		internalConsumer = new AbstractStreamConsumer<I>(eventloop) {
			@Override
			public StreamDataReceiver<I> getDataReceiver() {
				return AbstractStreamTransformer_1_1.this.getInternalDataReceiver();
			}

			@Override
			protected void onClosed() {
				AbstractStreamTransformer_1_1.this.onUpstreamProducerClosed();
			}
		};

		internalProducer = new AbstractStreamProducer<O>(eventloop) {
			@Override
			protected void onSuspended() {
				AbstractStreamTransformer_1_1.this.onDownstreamConsumerSuspended();
			}

			@Override
			protected void onResumed() {
				AbstractStreamTransformer_1_1.this.onDownstreamConsumerResumed();
			}

			// TODO (vsavchuk) глянути які методи використовуються при наслідуванні
		};
	}

	protected abstract void onUpstreamProducerClosed();

//	@Override
//	public StreamConsumer<I> getInput() {
//		return internalConsumer;
//	}
//
//	@Override
//	public StreamProducer<O> getOutput() {
//		return internalProducer;
//	}

	protected abstract StreamDataReceiver<I> getInternalDataReceiver();

	@Override
	public final StreamDataReceiver<I> getDataReceiver() {
		return internalConsumer.getDataReceiver();
	}

	@Override
	public final void onProducerEndOfStream() {
		internalConsumer.onProducerEndOfStream();
	}

	@Override
	public final void addConsumerCompletionCallback(CompletionCallback completionCallback) {
		internalConsumer.addConsumerCompletionCallback(completionCallback);
	}

	@Override
	public final void streamTo(StreamConsumer<O> downstreamConsumer) {
		internalProducer.streamTo(downstreamConsumer);
	}

	@Override
	public final StreamConsumer<O> getDownstream() {
		return internalProducer.getDownstream();
	}

	@Override
	public final void onConsumerSuspended() {
		internalProducer.onConsumerSuspended();
	}

	@Override
	public final void onConsumerResumed() {
		internalProducer.onConsumerResumed();
	}

	@Override
	public final void onConsumerError(Exception e) {
		internalProducer.onConsumerError(e);
	}

	@Override
	public final void addProducerCompletionCallback(CompletionCallback completionCallback) {
		internalProducer.addProducerCompletionCallback(completionCallback);
	}

	@Override
	public final void onProducerError(Exception e) {
		internalConsumer.onProducerError(e);
	}

	@Override
	public final void setUpstream(StreamProducer<I> upstreamProducer) {
		internalConsumer.setUpstream(upstreamProducer);
	}

	@Override
	public final StreamProducer<I> getUpstream() {
		return internalConsumer.getUpstream();
	}

	@Override
	public final void bindDataReceiver() {
		internalProducer.bindDataReceiver();
	}

	//	@Override
//	public void setUpstream(final StreamProducer<I> upstreamProducer) {
//		checkNotNull(upstreamProducer);
//		checkState(this.upstreamProducer == null, "Already wired");
//		this.upstreamProducer = upstreamProducer;
//
//		addConsumerCompletionCallback(new CompletionCallback() {
//			@Override
//			public void onComplete() {
//
//			}
//
//			@Override
//			public void onException(Exception exception) {
//				upstreamProducer.onConsumerError(exception);
//				downstreamConsumer.onProducerError(exception);
//				closedWithError(exception);
//			}
//		});
//
//		eventloop.post(new Runnable() {
//			@Override
//			public void run() {
//				onConsumerStarted();
//			}
//		});
//	}

//	protected void onConsumerStarted() {
//	}
//
//	@Override
//	public void bindDataReceiver() {
//		super.bindDataReceiver();
//		if (upstreamProducer != null) {
//			upstreamProducer.bindDataReceiver();
//		}
//	}
//
//	@Override
//	@Nullable
//	public StreamProducer<I> getUpstream() {
//		return upstreamProducer;
//	}
//
//	public byte getUpstreamStatus() {
//		return ((AbstractStreamProducer)upstreamProducer).getStatus();
//	}
//
//	@Override
//	public void onClosed() {
////		upstreamProducer.close();
////		downstreamConsumer.onProducerEndOfStream();
//		// TODO (vsavchuk) поудаляти всі System.out.println();
//		System.out.println("Transformer onClosed()");
//		close();
//	}
//
////	@Override
////	protected void onClosedWithError(Exception e) {
////		upstreamProducer.onConsumerError(e);
////		downstreamConsumer.onProducerError(e);
////		closedWithError(e);
////	}
//
//	@Override
//	public void onProducerError(Exception e) {
//		downstreamConsumer.onProducerError(e);
//		upstreamProducer.onConsumerError(e);
//		closedWithError(e);
//	}
//
//	protected final void resumeUpstream() {
//		upstreamProducer.onConsumerResumed();
//	}
//
//	protected final void suspendUpstream() {
//		upstreamProducer.onConsumerSuspended();
//	}
//
////	protected final void closeUpstream() {
////		upstreamProducer.close();
////	}
//
//	protected final void closeUpstreamWithError(Exception e) {
//		upstreamProducer.onConsumerError(e);
//		closedWithError(e);
//	}

	// misc

	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

	protected abstract void onDownstreamConsumerSuspended();

	protected abstract void onDownstreamConsumerResumed();

	protected abstract void onUpstreamProducerEndOfStream();
}
