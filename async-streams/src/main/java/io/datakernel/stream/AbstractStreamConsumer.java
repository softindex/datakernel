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
import static io.datakernel.stream.AbstractStreamConsumer.StreamConsumerStatus.*;

/**
 * It is basic implementation of {@link StreamConsumer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	private static final Logger logger = LoggerFactory.getLogger(AbstractStreamConsumer.class);

	protected final Eventloop eventloop;

	protected StreamProducer<T> upstreamProducer;

	public enum StreamConsumerStatus {
		READY, SUSPENDED, END_OF_STREAM, CLOSED, CLOSED_WITH_ERROR;

		public boolean isOpen() {
			return this.ordinal() <= SUSPENDED.ordinal();
		}

		public boolean isEnded() {
			return this.ordinal() >= END_OF_STREAM.ordinal();
		}

		public boolean isClosed() {
			return this.ordinal() >= CLOSED.ordinal();
		}
	}

	private StreamConsumerStatus status = READY;
	protected Exception error;

	private final List<CompletionCallback> completionCallbacks = new ArrayList<>();

	private boolean rewiring;

	protected Object tag;

	protected AbstractStreamConsumer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

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

		if (status.isEnded()) {
			upstreamProducer.onConsumerError(new Exception("Connection to closed consumer"));
			return;
		}

		this.upstreamProducer = upstreamProducer;

		if (status == READY) {
			this.upstreamProducer.onConsumerResumed();
		}

		if (status == SUSPENDED) {
			this.upstreamProducer.onConsumerSuspended();
		}

		upstreamProducer.streamTo(this);

		if (firstTime) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					onStarted();
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

	protected final void bindUpstream() {
		if (upstreamProducer != null) {
			upstreamProducer.bindDataReceiver();
		}
	}

	@Override
	public final void addConsumerCompletionCallback(final CompletionCallback completionCallback) {
		checkNotNull(completionCallback);
		checkArgument(!completionCallbacks.contains(completionCallback));
		if (status.isClosed()) {
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
			if (upstreamProducer != null) {
				upstreamProducer.onConsumerSuspended();
			}
		}
	}

	protected void resume() {
		if (status == SUSPENDED) {
			status = READY;
			if (upstreamProducer != null) {
				upstreamProducer.onConsumerResumed();
			}
		}
	}

	protected void close() {
		if (status.isClosed())
			return;
		if (status != END_OF_STREAM) {
			logger.warn("Cancelling consumer {}", this);
			closeWithError(new Exception("Cancel"), true);
			return;
		}

		status = CLOSED;
		for (CompletionCallback callback : completionCallbacks) {
			callback.onComplete();
		}
		completionCallbacks.clear();
		onClosed();
	}

	protected void onClosed() {
	}

	private void closeWithError(Exception e, boolean sendToProducer) {
		if (status.isClosed())
			return;

		status = CLOSED_WITH_ERROR;
		error = e;
		logger.info("StreamConsumer {} closed with error {}", this, e.toString());
		if (sendToProducer) {
			if (upstreamProducer != null) {
				upstreamProducer.onConsumerError(e);
			}
		}
		for (CompletionCallback callback : completionCallbacks) {
			callback.onException(e);
		}
		completionCallbacks.clear();
		onError(e);
	}

	protected void closeWithError(Exception e) {
		closeWithError(e, true);
	}

	@Override
	public final void onProducerEndOfStream() {
		if (status.isEnded())
			return;
		status = END_OF_STREAM;

		onEndOfStream();
	}

	public final StreamConsumerStatus getStatus() {
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
