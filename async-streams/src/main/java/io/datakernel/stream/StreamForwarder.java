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

/**
 * Simple no-op forwarder of stream data, which supports wiring of itself to upstream producer or to downstream consumer at later time.
 * <p>After both upstream producer and downstream consumer are connected, StreamForwarder switches to zero-overhead 'short-circuit' mode, which directly forwards StreamDataReceiver from downstream to upstream.
 *
 * @param <T> type of data
 */
public class StreamForwarder<T> extends AbstractStreamTransformer_1_1<T, T> implements StreamDataReceiver<T> {
	private final ArrayList<T> bufferedItems = new ArrayList<>();
	private boolean bufferedEndOfStream;

	private boolean rewired;

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop eventloop in which forwarder will be running
	 */
	public StreamForwarder(Eventloop eventloop) {
		super(eventloop);

		addConsumerCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {

			}

			@Override
			public void onException(Exception exception) {
				internalConsumer.getUpstream().onConsumerError(exception);
				internalProducer.getDownstream().onProducerError(exception);
			}
		});

		addProducerCompletionCallback(new CompletionCallback() {
			@Override
			public void onComplete() {

			}

			@Override
			public void onException(Exception exception) {
				internalConsumer.getUpstream().onConsumerError(exception);
				internalProducer.getDownstream().onProducerError(exception);
			}
		});
	}

	@Override
	protected StreamDataReceiver<T> getInternalDataReceiver() {
		return rewired ? internalProducer.getDownstreamDataReceiver() : this; // short-circuit upstream producer and downstream consumer
	}
//
	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return rewired ? internalProducer.getDownstreamDataReceiver() : this; // short-circuit upstream producer and downstream consumer
	}

	@Override
	public void onData(T item) {
		if (rewired) {
			// Maybe downstreamDataReceiver is cached somewhere in upstream producer, even after rewire?
			// Anyway, let's just forward items to downstream.
			internalProducer.send(item);
		} else {
			bufferedItems.add(item);
			if (getUpstream() != null) {
				internalConsumer.getUpstream().onConsumerSuspended();
			}
		}
	}

	private void flushAndRewire() {
		if (error != null) {
			upstreamProducer.onConsumerError(error);
			downstreamConsumer.onProducerError(error);
			return;
		}

		for (T item : bufferedItems) {
			send(item);
		}
		bufferedItems.clear();

		if (bufferedEndOfStream) {
			sendEndOfStream();
		}

		rewired = true;
		getUpstream().bindDataReceiver();

		if (status == CLOSED) {
			closeUpstream();
		} else if (status != READY) {
			suspendUpstream();
		} else {
			resumeUpstream();
		}
	}

	@Override
	protected void onConsumerStarted() {
		if (getUpstream() != null && getDownstream() != null) {
			flushAndRewire();
		}
	}

	@Override
	protected void onProducerStarted() {
		if (getUpstream() != null && getDownstream() != null) {
			flushAndRewire();
		}
	}

	@Override
	public void onProducerEndOfStream() {
		if (rewired) {
			sendEndOfStream();
		} else {
			bufferedEndOfStream = true;
		}
	}

	@Override
	public void onConsumerSuspended() {
		if (rewired) {
			internalConsumer.getUpstream().onConsumerSuspended();
		}
	}

	@Override
	public void onConsumerResumed() {
		if (rewired) {
			internalConsumer.getUpstream().onConsumerResumed();
		}
	}

	@Override
	public void onClosed() {
		if (rewired) {
			closeUpstream();
		}
	}

	@Override
	public void onClosedWithError(Exception e) {
		if (rewired) {
			internalConsumer.getUpstream().onConsumerError(e);
			internalProducer.getDownstream().onProducerError(e);
		} else {
			internalProducer.error = e;
		}
	}


}
