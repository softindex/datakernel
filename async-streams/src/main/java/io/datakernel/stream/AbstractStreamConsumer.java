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

	private final List<CompletionCallback> completionCallbacks = new ArrayList<>();

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
	public void setUpstream(final StreamProducer<T> upstreamProducer) {
		checkNotNull(upstreamProducer);
		checkState(this.upstreamProducer == null, "Already wired");
		this.upstreamProducer = upstreamProducer;

		eventloop.post(new Runnable() {
			@Override
			public void run() {
				for (CompletionCallback completionCallback : completionCallbacks) {
					upstreamProducer.addCompletionCallback(completionCallback);
				}
				completionCallbacks.clear();
				onConsumerStarted();
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
	public void onError(Exception e) {
		upstreamProducer.closeWithError(e);
	}

	@Override
	public void addCompletionCallback(final CompletionCallback completionCallback) {
		checkNotNull(completionCallback);
		checkArgument(!completionCallbacks.contains(completionCallback));
		if (upstreamProducer != null) {
			upstreamProducer.addCompletionCallback(completionCallback);
		} else {
			completionCallbacks.add(completionCallback);
		}
	}

	public byte getUpstreamStatus() {
		return upstreamProducer.getStatus();
	}

	public void suspendUpstream() {
		upstreamProducer.suspend();
	}

	public void resumeUpstream() {
		upstreamProducer.resume();
	}

	public void closeUpstream() {
		upstreamProducer.close();
	}

	public void closeUpstreamWithError(Exception e) {
		upstreamProducer.closeWithError(e);
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
