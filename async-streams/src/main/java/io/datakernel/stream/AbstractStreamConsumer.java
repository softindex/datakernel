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

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.*;

/**
 * It is basic implementation of {@link StreamConsumer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {

	protected final Eventloop eventloop;

	protected StreamProducer<T> upstreamProducer;
	protected Exception error;

	private final List<CompletionCallback> completionCallbacks = new ArrayList<>();

	private boolean closed;
	protected Object tag;

	protected AbstractStreamConsumer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

//	public static final byte READY = 0;
//	public static final byte SUSPENDED = 1;
//	public static final byte END_OF_STREAM = 2;
//	public static final byte CLOSED = 3;
//	public static final byte CLOSED_WITH_ERROR = 4;

	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param upstreamProducer stream producer for setting
	 */

	// TODO(vsavchuk) переробити на таку логіку статусів
	//	public static final byte READY = 0;
//	public static final byte CLOSED = 1;
//	public static final byte ERROR = 2;
//
	@Override
	public void setUpstream(final StreamProducer<T> upstreamProducer) {
		checkNotNull(upstreamProducer);
		checkState(this.upstreamProducer == null, "Already wired");
		this.upstreamProducer = upstreamProducer;

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				if (!closed) {
					onConsumerStarted();
				}
			}
		});
	}

	protected void onConsumerStarted() {
	}

	@Override
	@Nullable
	public StreamProducer<T> getUpstream() {
		return upstreamProducer;
	}

	@Override
	public void onProducerEndOfStream() {
		close();
	}

	@Override
	public void onProducerError(Exception e) {
		closeWithError(e);
	}

	@Override
	public void addConsumerCompletionCallback(final CompletionCallback completionCallback) {
		checkNotNull(completionCallback);
		checkArgument(!completionCallbacks.contains(completionCallback));
		if (closed) {
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					if (error == null) {
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

	public void suspendUpstream() {
		upstreamProducer.onConsumerSuspended();
	}

	public void resumeUpstream() {
		upstreamProducer.onConsumerResumed();
	}

//	public void closeUpstream() {
//		status = CLOSED;
////		upstreamProducer.close();
//		close();
//	}

//	public void closeUpstreamWithError(Exception e) {
//		status = CLOSED_WITH_ERROR;
////		upstreamProducer.onConsumerError(e);
//		closeWithError(e);
//	}

	protected void close() {
		if (closed)
			return;

		closed = true;
		for (CompletionCallback callback : completionCallbacks) {
			callback.onComplete();
		}
		completionCallbacks.clear();
		onClosed();
	}

	protected void onClosed() {

	}

	protected void closeWithError(Exception e) {
		if (closed)
			return;

		closed = true;
		error = e;
		upstreamProducer.onConsumerError(e);
		for (CompletionCallback callback : completionCallbacks) {
			callback.onException(e);
		}
		completionCallbacks.clear();
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
