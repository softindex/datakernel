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

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents {@link AbstractStreamProducer} with few {@link AbstractStreamConsumer}.
 *
 * @param <O> type of sending items
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_M_1<O> implements StreamProducer<O> {
	protected final List<AbstractStreamConsumer<?>> internalConsumers = new ArrayList<>();
	protected final AbstractStreamProducer internalProducer;
	protected final int countEndOfStreams = 0;
	protected final Eventloop eventloop;

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this producer will run
	 */
	public AbstractStreamTransformer_M_1(Eventloop eventloop) {
		this.eventloop = eventloop;
		internalProducer = new AbstractStreamProducer(eventloop) {};
	}

	@Override
	public void bindDataReceiver() {
		internalProducer.bindDataReceiver();
		for (AbstractStreamConsumer<?> input : internalConsumers) {
			if (input.getUpstream() != null) {
				input.getUpstream().bindDataReceiver();
			}
		}
	}

	protected void onInternalError(Exception e) {
//		onConsumerError(e);
		internalProducer.getDownstream().onProducerError(e);
		internalProducer.sendError(e);
		for (AbstractStreamConsumer<?> input : internalConsumers) {
//			input.closeUpstreamWithError(e);
			input.onProducerError(e);
		}
	}

//	/**
//	 * Action which will take place after changing status to complete
//	 */
//	@Override
//	protected void onClosed() {
////		closeAllUpstreams();
//	}

//	/**
//	 * If consumer has exception, all internalConsumers handle this exception
//	 */
//	@Override
//	protected void onClosedWithError(Exception e) {
//		downstreamConsumer.onProducerError(e);
//		for (AbstractStreamConsumer<?> input : internalConsumers) {
//			input.onProducerError(e);
//		}
//	}

	/**
	 * Adds a new stream consumer to this producer
	 *
	 * @param streamConsumer stream consumer for adding
	 * @param <T>            type of stream consumer
	 * @return new stream consumer
	 */
	protected <T extends AbstractStreamConsumer<?>> T addInput(T streamConsumer) {
		checkNotNull(streamConsumer);
		internalConsumers.add(streamConsumer);
		return streamConsumer;
	}

	protected void suspendAllUpstreams() {
		for (AbstractStreamConsumer<?> input : internalConsumers) {
			input.suspendUpstream();
		}
	}

	protected void resumeAllUpstreams() {
		for (AbstractStreamConsumer<?> input : internalConsumers) {
			input.resumeUpstream();
		}
	}

//	protected void closeAllUpstreams() {
//		for (AbstractStreamConsumer<?> input : internalConsumers) {
//			input.closeUpstream();
//			input.onProducerError();
//		}
//	}


	// переробити
	protected boolean allUpstreamsEndOfStream() {
		for (AbstractStreamConsumer<?> input : internalConsumers) {
			if (input.getUpstream() == null || countEndOfStreams != internalConsumers.size())
				return false;
		}
		return true;
	}

	@Override
	public void streamTo(StreamConsumer<O> downstreamConsumer) {
		internalProducer.streamTo(downstreamConsumer);
	}

	@Override
	public StreamConsumer<O> getDownstream() {
		return internalProducer.getDownstream();
	}

	@Override
	public void onConsumerSuspended() {
		suspendAllUpstreams();
	}

	@Override
	public void onConsumerResumed() {
		resumeAllUpstreams();
	}

	@Override
	public void onConsumerError(Exception e) {
		internalProducer.onConsumerError(e);
	}

	@Override
	public final void addProducerCompletionCallback(CompletionCallback completionCallback) {
		internalProducer.addProducerCompletionCallback(completionCallback);
	}
}
