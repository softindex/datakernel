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
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.stream.StreamStatus.*;

/**
 * It is basic implementation of {@link StreamConsumer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T>, HasEndOfStream {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected final Eventloop eventloop;

	private StreamProducer<T> producer;

	private StreamStatus status = READY;
	private Throwable exception;

	private final SettableStage<Void> endOfStream = SettableStage.create();

	private Object tag;

	protected AbstractStreamConsumer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	/**
	 * Sets wired producer. It will sent data to this consumer
	 *
	 * @param producer stream producer for setting
	 */

	@Override
	public final void setProducer(StreamProducer<T> producer) {
		checkNotNull(producer);
		checkState(this.producer == null);
		this.producer = producer;
		onWired();
	}

	protected void onWired() {
		eventloop.post(this::onStarted);
	}

	protected void onStarted() {
	}

	public boolean isWired() {
		return producer != null;
	}

	@Nullable
	public final StreamProducer<T> getProducer() {
		return producer;
	}

	@Override
	public final void endOfStream() {
		if (status.isClosed())
			return;
		status = END_OF_STREAM;

		onEndOfStream();
		eventloop.post(this::cleanup);
		endOfStream.set(null);
	}

	protected abstract void onEndOfStream();

	@Override
	public final void closeWithError(Throwable t) {
		if (status.isClosed())
			return;
		status = CLOSED_WITH_ERROR;
		exception = t;
		if (!(t instanceof ExpectedException)) {
			if (logger.isWarnEnabled()) {
				logger.warn("StreamConsumer {} closed with error {}", this, t.toString());
			}
		}
		producer.closeWithError(t);
		onError(t);
		eventloop.post(this::cleanup);
		endOfStream.setException(t);
	}

	protected abstract void onError(Throwable t);

	protected void cleanup() {
	}

	public final StreamStatus getStatus() {
		return status;
	}

	public final Throwable getException() {
		return exception;
	}

	@Override
	public final CompletionStage<Void> getEndOfStream() {
		return endOfStream;
	}

	public final Object getTag() {
		return tag;
	}

	public final void setTag(Object tag) {
		this.tag = tag;
	}

	@Override
	public String toString() {
		return tag != null ? tag.toString() : super.toString();
	}

}
