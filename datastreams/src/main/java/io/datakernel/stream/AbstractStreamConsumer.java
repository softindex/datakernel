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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.datakernel.stream.StreamStatus.*;

/**
 * It is basic implementation of {@link StreamConsumer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected final Eventloop eventloop;

	private StreamProducer<T> producer;

	private StreamStatus status = READY;
	private Exception error;

	private final SettableStage<Void> completionStage = SettableStage.create();

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
	public final void streamFrom(StreamProducer<T> producer) {
		checkNotNull(producer);
		if (this.producer == producer) return;

		checkState(this.producer == null);
		checkState(status == READY);

		this.producer = producer;

		producer.streamTo(this);

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
		completionStage.set(null);
	}

	public final StreamStatus getStatus() {
		return status;
	}

	@Override
	public final void closeWithError(Exception e) {
		if (status.isClosed())
			return;
		status = CLOSED_WITH_ERROR;
		error = e;
		if (!(e instanceof ExpectedException)) {
			if (logger.isWarnEnabled()) {
				logger.warn("StreamConsumer {} closed with error {}", this, e.toString());
			}
		}
		producer.closeWithError(e);
		onError(e);
		eventloop.post(this::cleanup);
		completionStage.setException(e);
	}

	protected abstract void onEndOfStream();

	protected abstract void onError(Exception e);

	protected void cleanup() {
	}

	public final Exception getException() {
		return error;
	}

	public final SettableStage<Void> getCompletionStage() {
		return completionStage;
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
