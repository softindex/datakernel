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

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.StreamTransformer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Represent {@link StreamProducer} and {@link StreamConsumer} in the one object.
 * This object can receive and send streams of data.
 *
 * @param <I> type of input data for consumer
 * @param <O> type of output data of producer
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_1_1<I, O> extends AbstractStreamProducer<O> implements StreamConsumer<I>, StreamTransformer<I, O> {
	protected StreamProducer<I> upstreamProducer;

	protected Object tag;

	protected AbstractStreamTransformer_1_1(Eventloop eventloop) {
		super(eventloop);
	}

	@Override
	public void setUpstream(StreamProducer<I> upstreamProducer) {
		checkNotNull(upstreamProducer);
		checkState(this.upstreamProducer == null, "Already wired");
		this.upstreamProducer = upstreamProducer;

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				onConsumerStarted();
			}
		});
	}

	protected void onConsumerStarted() {
	}

	@Override
	public void bindDataReceiver() {
		super.bindDataReceiver();
		if (upstreamProducer != null) {
			upstreamProducer.bindDataReceiver();
		}
	}

	@Override
	@Nullable
	public StreamProducer<I> getUpstream() {
		return upstreamProducer;
	}

	public byte getUpstreamStatus() {
		return upstreamProducer.getStatus();
	}

	@Override
	public void onClosed() {
		upstreamProducer.close();
	}

	@Override
	protected void onClosedWithError(Exception e) {
		upstreamProducer.closeWithError(e);
		downstreamConsumer.onError(e);
	}

	@Override
	public void onError(Exception e) {
		closeWithError(e);
	}

	protected final void resumeUpstream() {
		upstreamProducer.resume();
	}

	protected final void suspendUpstream() {
		upstreamProducer.suspend();
	}

	protected final void closeUpstream() {
		upstreamProducer.close();
	}

	protected final void closeUpstreamWithError(Exception e) {
		upstreamProducer.closeWithError(e);
	}

	// misc

	@Override
	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

}
