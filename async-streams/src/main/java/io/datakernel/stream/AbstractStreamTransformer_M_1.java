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

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents {@link AbstractStreamProducer} with few {@link AbstractStreamConsumer}.
 *
 * @param <O> type of sending items
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStreamTransformer_M_1<O> extends AbstractStreamProducer<O> {
	protected final List<AbstractStreamConsumer<?>> inputs = new ArrayList<>();

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this producer will run
	 */
	public AbstractStreamTransformer_M_1(Eventloop eventloop) {
		super(eventloop);
	}

	@Override
	public void bindDataReceiver() {
		super.bindDataReceiver();
		for (AbstractStreamConsumer<?> input : inputs) {
			if (input.getUpstream() != null) {
				input.getUpstream().bindDataReceiver();
			}
		}
	}

	@Override
	protected void onInternalError(Exception e) {
		onConsumerError(e);
		sendError(e);
		for (AbstractStreamConsumer<?> input : inputs) {
			input.closeUpstreamWithError(e);
		}
	}

	/**
	 * Action which will take place after changing status to complete
	 */
	@Override
	protected void onClosed() {
		closeAllUpstreams();
	}

	/**
	 * If consumer has exception, all inputs handle this exception
	 */
	@Override
	protected void onClosedWithError(Exception e) {
		downstreamConsumer.onProducerError(e);
		for (AbstractStreamConsumer<?> input : inputs) {
			input.onProducerError(e);
		}
	}

	/**
	 * Adds a new stream consumer to this producer
	 *
	 * @param streamConsumer stream consumer for adding
	 * @param <T>            type of stream consumer
	 * @return new stream consumer
	 */
	protected <T extends AbstractStreamConsumer<?>> T addInput(T streamConsumer) {
		checkNotNull(streamConsumer);
		inputs.add(streamConsumer);
		return streamConsumer;
	}

	protected void suspendAllUpstreams() {
		for (AbstractStreamConsumer<?> input : inputs) {
			input.suspendUpstream();
		}
	}

	protected void resumeAllUpstreams() {
		for (AbstractStreamConsumer<?> input : inputs) {
			input.resumeUpstream();
		}
	}

	protected void closeAllUpstreams() {
		for (AbstractStreamConsumer<?> input : inputs) {
			input.closeUpstream();
		}
	}

	protected boolean allUpstreamsEndOfStream() {
		for (AbstractStreamConsumer<?> input : inputs) {
			if (input.getUpstream() == null || input.getUpstreamStatus() != END_OF_STREAM)
				return false;
		}
		return true;
	}
}
