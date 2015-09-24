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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
	public static final byte CLOSED_WITH_ERROR = 4;

	private boolean rewiring;

	/**
	 * Sets consumer for this producer. At the moment of calling this method producer shouldn't have consumer,
	 * as well as consumer shouldn't have producer, otherwise there will be error
	 *
	 * @param downstreamConsumer consumer for streaming
	 */
	@Override
	public final void streamTo(StreamConsumer<T> downstreamConsumer) {
		checkNotNull(downstreamConsumer);
		if (rewiring || this.downstreamConsumer == downstreamConsumer)
			return;
		rewiring = true;

		boolean firstTime = this.downstreamConsumer == null;

		if (this.downstreamConsumer != null) {
			this.downstreamConsumer.streamFrom(StreamProducers.<T>closingWithError(eventloop,
					new Exception("Downstream disconnected")));
		}

		this.downstreamConsumer = downstreamConsumer;

		downstreamConsumer.streamFrom(this);

		bindDataReceiver();

		if (firstTime) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (status < END_OF_STREAM) {
						onStarted();
					}
				}
			});
		}
		rewiring = false;
	}

	/**
	 * Sends {@code item} to consumer
	 *
	 * @param item item to be sent
	 */
	protected void send(T item) {
		assert status < END_OF_STREAM;
		downstreamDataReceiver.onData(item);
	}

	protected void doProduce() {
	}

	protected void produce() {
		if (status != READY)
			return;
		try {
			doProduce();
		} catch (Exception e) {
			closeWithError(e, true);
			onError(e);
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

	protected abstract void onStarted();

	@Nullable
	public final StreamConsumer<T> getDownstream() {
		return downstreamConsumer;
	}

	/**
	 * Connects consumer's {@link StreamDataReceiver} to producer
	 */
	@Override
	public final void bindDataReceiver() {
		StreamDataReceiver<T> newDataReceiver = downstreamConsumer.getDataReceiver();
		if (downstreamDataReceiver != newDataReceiver) {
			downstreamDataReceiver = newDataReceiver;
			onDataReceiverChanged();
		}
	}

	protected abstract void onDataReceiverChanged();

	public StreamDataReceiver<T> getDownstreamDataReceiver() {
		return downstreamDataReceiver;
	}

	abstract protected void onSuspended();

	@Override
	public final void onConsumerSuspended() {
		if (status != READY)
			return;
		status = SUSPENDED;
		onSuspended();
	}

	abstract protected void onResumed();

	@Override
	public final void onConsumerResumed() {
		if (status != SUSPENDED)
			return;
		status = READY;
		onResumed();
	}

	protected void sendEndOfStream() {
		if (status >= END_OF_STREAM)
			return;
		status = END_OF_STREAM;
		downstreamConsumer.onProducerEndOfStream();
		for (CompletionCallback callback : completionCallbacks) {
			callback.onComplete();
		}
		completionCallbacks.clear();
	}

	private void closeWithError(Exception e, boolean sendToConsumer) {
		if (status >= END_OF_STREAM)
			return;
		status = CLOSED_WITH_ERROR;
		error = e;
		logger.error("StreamProducer {} closed with error", this, e);
		if (sendToConsumer) {
			downstreamConsumer.onProducerError(e);
		}
		for (CompletionCallback callback : completionCallbacks) {
			callback.onException(e);
		}
		completionCallbacks.clear();
	}

	protected void closeWithError(Exception e) {
		closeWithError(e, true);
	}

	@Override
	public final void onConsumerError(Exception e) {
		closeWithError(e, false);
		onError(e);
	}

	protected abstract void onError(Exception e);

	/**
	 * Returns current status of this producer
	 *
	 * @return current status of this producer
	 */
	public final byte getStatus() {
		return status;
	}

	@Override
	public final void addProducerCompletionCallback(final CompletionCallback completionCallback) {
		checkNotNull(completionCallback);
		checkArgument(!completionCallbacks.contains(completionCallback));
		if (status >= END_OF_STREAM) {
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
