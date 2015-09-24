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
 * It is basic implementation of {@link StreamConsumer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	private static final Logger logger = LoggerFactory.getLogger(AbstractStreamConsumer.class);

	protected final Eventloop eventloop;

	protected StreamProducer<T> upstreamProducer;
	protected Exception error;

	private final List<CompletionCallback> completionCallbacks = new ArrayList<>();

	private byte status;
	protected Object tag;

	protected AbstractStreamConsumer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	public static final byte READY = 0;
	public static final byte SUSPENDED = 1;
	public static final byte END_OF_STREAM = 2;
	public static final byte CLOSED = 3;
	public static final byte CLOSED_WITH_ERROR = 4;

	private boolean rewiring;

	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param upstreamProducer stream producer for setting
	 */


	@Override
	public final void streamFrom(StreamProducer<T> upstreamProducer) {
		checkNotNull(upstreamProducer);
		if (rewiring || this.upstreamProducer == upstreamProducer)
			return;
		rewiring = true;

		boolean firstTime = this.upstreamProducer == null;

		if (this.upstreamProducer != null) {
			this.upstreamProducer.streamTo(StreamConsumers.<T>closingWithError(eventloop,
					new Exception("Downstream disconnected")));
		}

		this.upstreamProducer = upstreamProducer;

		upstreamProducer.streamTo(this);

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

	abstract protected void onStarted();

	@Nullable
	public final StreamProducer<T> getUpstream() {
		return upstreamProducer;
	}

	@Override
	public final void addConsumerCompletionCallback(final CompletionCallback completionCallback) {
		checkNotNull(completionCallback);
		checkArgument(!completionCallbacks.contains(completionCallback));
		if (status >= CLOSED) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (status == CLOSED) {
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

	protected void suspend() {
		if (status == READY) {
			status = SUSPENDED;
			upstreamProducer.onConsumerSuspended();
		}
	}

	protected void resume() {
		if (status == SUSPENDED) {
			status = READY;
			upstreamProducer.onConsumerResumed();
		}
	}

	protected void close() {
		if (status >= CLOSED)
			return;

		status = CLOSED;
		for (CompletionCallback callback : completionCallbacks) {
			callback.onComplete();
		}
		completionCallbacks.clear();
	}

	private void closeWithError(Exception e, boolean internal) {
		if (status >= CLOSED)
			return;

		status = CLOSED_WITH_ERROR;
		error = e;
		if (internal) {
			upstreamProducer.onConsumerError(e);
		}
		for (CompletionCallback callback : completionCallbacks) {
			callback.onException(e);
		}
		completionCallbacks.clear();
		if (!internal) {
			onError(e);
		}
	}

	protected void closeWithError(Exception e) {
		closeWithError(e, true);
	}

	@Override
	public final void onProducerEndOfStream() {
		if (status >= END_OF_STREAM)
			return;
		status = END_OF_STREAM;

		onEndOfStream();
	}

	public byte getStatus() {
		return status;
	}

	abstract protected void onEndOfStream();

	@Override
	public final void onProducerError(Exception e) {
		closeWithError(e, false);
	}

	protected abstract void onError(Exception e);

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
