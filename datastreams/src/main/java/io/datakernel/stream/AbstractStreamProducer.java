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
 * It is basic implementation of {@link StreamProducer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamProducer<T> implements StreamProducer<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected final Eventloop eventloop;

	private StreamConsumer<T> consumer;

	private StreamStatus status = SUSPENDED;
	private Exception error;

	private StreamDataReceiver<T> currentDataReceiver;
	private StreamDataReceiver<T> lastDataReceiver;
	private boolean producing;
	private boolean posted;

	private final SettableStage<Void> completionStage = SettableStage.create();

	private Object tag;

	protected AbstractStreamProducer(Eventloop eventloop) {
		this.eventloop = checkNotNull(eventloop);
	}

	/**
	 * Sets consumer for this producer. At the moment of calling this method producer shouldn't have consumer,
	 * as well as consumer shouldn't have producer, otherwise there will be error
	 *
	 * @param consumer consumer for streaming
	 */
	@Override
	public final void streamTo(final StreamConsumer<T> consumer) {
		checkNotNull(consumer);
		if (this.consumer == consumer) return;
		checkState(this.consumer == null);
		checkState(status == SUSPENDED);

		this.consumer = consumer;

		consumer.streamFrom(this);

		onWired();
	}

	protected void onWired() {
		eventloop.post(this::onStarted);
	}

	protected void onStarted() {
	}

	public boolean isWired() {
		return consumer != null;
	}

	@Nullable
	public final StreamConsumer<T> getConsumer() {
		return consumer;
	}

	public final boolean isReceiverReady() {
		return currentDataReceiver != null;
	}

	public final void send(T item) {
		currentDataReceiver.onData(item);
	}

	@Nullable
	public final StreamDataReceiver<T> getCurrentDataReceiver() {
		return currentDataReceiver;
	}

	@Nullable
	public StreamDataReceiver<T> getLastDataReceiver() {
		return lastDataReceiver;
	}

	protected void produce() {
	}

	protected void onProduce(StreamDataReceiver<T> dataReceiver) {
		if (producing)
			return; // recursive call from downstream - just hot-switch to another receiver
		if (posted)
			return;
		posted = true;
		eventloop.post(() -> {
			posted = false;
			if (!isReceiverReady())
				return;
			producing = true;
			produce();
			producing = false;
		});
	}

	@Override
	public final void produce(StreamDataReceiver<T> dataReceiver) {
		assert dataReceiver != null;
		if (currentDataReceiver == dataReceiver)
			return;
		if (status.isClosed())
			return;
		status = READY;
		currentDataReceiver = dataReceiver;
		lastDataReceiver = dataReceiver;
		onProduce(dataReceiver);
	}

	protected void onSuspended() {
	}

	@Override
	public final void suspend() {
		if (!isReceiverReady())
			return;
		status = SUSPENDED;
		currentDataReceiver = null;
		onSuspended();
	}

	public void sendEndOfStream() {
		if (status.isClosed())
			return;
		status = END_OF_STREAM;
		currentDataReceiver = null;
		lastDataReceiver = item -> {};
		consumer.endOfStream();
		eventloop.post(this::cleanup);
		completionStage.set(null);
	}

	@Override
	public final void closeWithError(Exception e) {
		if (status.isClosed())
			return;
		status = CLOSED_WITH_ERROR;
		currentDataReceiver = null;
		lastDataReceiver = item -> {};
		error = e;
		if (!(e instanceof ExpectedException)) {
			if (logger.isWarnEnabled()) {
				logger.warn("StreamProducer {} closed with error {}", this, error.toString());
			}
		}
		consumer.closeWithError(e);
		onError(e);
		eventloop.post(this::cleanup);
		completionStage.setException(e);
	}

	protected abstract void onError(Exception e);

	protected void cleanup() {
	}

	public final StreamStatus getStatus() {
		return status;
	}

	public final Exception getException() {
		return error;
	}

	public final SettableStage<Void> getCompletionStage() {
		return completionStage;
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
