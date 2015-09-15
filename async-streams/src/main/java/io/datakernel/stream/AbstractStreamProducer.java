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
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.*;

/**
 * It is basic implementation of {@link StreamProducer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamProducer<T> implements StreamProducer<T> {
	private static final Logger logger = LoggerFactory.getLogger(AbstractStreamProducer.class);

	protected final Eventloop eventloop;

	protected StreamConsumer<T> downstreamConsumer;
	protected StreamDataReceiver<T> downstreamDataReceiver;

	protected byte status = READY;
	protected Exception error;

	private final List<CompletionCallback> completionCallbacks = new ArrayList<>();

	protected Object tag;

	protected AbstractStreamProducer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	public static final byte READY = 0;
	public static final byte SUSPENDED = 1;
	public static final byte END_OF_STREAM = 2;
	public static final byte CLOSED = 3;
	public static final byte CLOSED_WITH_ERROR = 4;

	/**
	 * Sets consumer for this producer. At the moment of calling this method producer shouldn't have consumer,
	 * as well as consumer shouldn't have producer, otherwise there will be error
	 *
	 * @param downstreamConsumer consumer for streaming
	 */
	@Override
	public void streamTo(StreamConsumer<T> downstreamConsumer) {
		checkNotNull(downstreamConsumer);
		checkState(this.downstreamConsumer == null, "Producer is already wired");
		checkArgument(downstreamConsumer.getUpstream() == null, "Consumer is already wired");

		this.downstreamConsumer = downstreamConsumer;

		downstreamConsumer.setUpstream(this);

		bindDataReceiver();

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				if (status < END_OF_STREAM) {
					onProducerStarted();
				}
			}
		});
	}

	/**
	 * Sends {@code item} to consumer
	 *
	 * @param item item to be sent
	 */
	public void send(T item) {
		assert status < END_OF_STREAM;
		downstreamDataReceiver.onData(item);
	}

	public final void sendEndOfStream() {
		if (status < END_OF_STREAM) {
			status = END_OF_STREAM;
			downstreamConsumer.onProducerEndOfStream();
		}
	}

	public final void sendError(Exception e) {
		downstreamConsumer.onProducerError(e);
	}

	protected void doProduce() {
	}

	protected final void produce() {
		if (status != READY)
			return;
		try {
			doProduce();
		} catch (Exception e) {
			onInternalError(e);
		}
	}

	protected void resumeProduce() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				produce();
			}
		});
	}

	protected void onProducerStarted() {
	}

	protected void onInternalError(Exception e) {
		onConsumerError(e);
	}

	@Override
	@Nullable
	public StreamConsumer<T> getDownstream() {
		return downstreamConsumer;
	}

	/**
	 * Connects consumer's {@link StreamDataReceiver} to producer
	 */
	@Override
	public void bindDataReceiver() {
		downstreamDataReceiver = downstreamConsumer.getDataReceiver();
	}

	public StreamDataReceiver<T> getDownstreamDataReceiver() {
		return downstreamDataReceiver;
	}

	protected void onSuspended() {
	}

	@Override
	public final void onConsumerSuspended() {
		if (status != READY)
			return;
		status = SUSPENDED;
		onSuspended();
	}

	protected void onResumed() {
	}

	@Override
	public final void onConsumerResumed() {
		if (status != SUSPENDED)
			return;
		status = READY;
		onResumed();
	}

	protected void onClosed() {
	}

	@Override
	public final void close() {
		if (status >= CLOSED)
			return;
		logger.trace("StreamProducer {} closed", this);
		status = CLOSED;
		for (CompletionCallback completionCallback : completionCallbacks) {
			completionCallback.onComplete();
		}
		completionCallbacks.clear();
		onClosed();
	}

	protected void onClosedWithError(Exception e) {
		downstreamConsumer.onProducerError(e);
	}

	@Override
	public final void onConsumerError(Exception e) {
		checkNotNull(e);
		if (status >= CLOSED)
			return;
		logger.error("StreamConsumer {} closed with error", this, e);
		status = CLOSED_WITH_ERROR;
		error = e;
		for (CompletionCallback completionCallback : completionCallbacks) {
			completionCallback.onException(e);
		}
		completionCallbacks.clear();
		onClosedWithError(e);
	}

	/**
	 * Returns current status of this producer
	 *
	 * @return current status of this producer
	 */
	public byte getStatus() {
		return status;
	}

	/**
	 * Returns exception from this consumer
	 */
	@Override
	public Exception getError() {
		return error;
	}

	@Override
	public void addCompletionCallback(final CompletionCallback completionCallback) {
		checkNotNull(completionCallback);
		checkArgument(!completionCallbacks.contains(completionCallback));
		if (status >= CLOSED) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (status != CLOSED_WITH_ERROR) {
						completionCallback.onComplete();
					} else {
						completionCallback.onException(error);
					}
				}
			});
			return;
		}
		completionCallbacks.add(completionCallback);
	}

	public Object getTag() {
		return tag;
	}

	public void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}
}
