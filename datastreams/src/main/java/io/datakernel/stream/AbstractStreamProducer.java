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
import io.datakernel.async.NextStage;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.stream.StreamStatus.*;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.emptySet;

/**
 * It is basic implementation of {@link StreamProducer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamProducer<T> implements StreamProducer<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final long createTick = eventloop.getTick();

	private StreamConsumer<T> consumer;

	private StreamStatus status = OPEN;
	private Throwable exception;

	private final SettableStage<Void> endOfStream = SettableStage.create();

	private StreamDataReceiver<T> currentDataReceiver;
	private StreamDataReceiver<T> lastDataReceiver;
	private boolean producing;
	private boolean posted;

	private Object tag;

	/**
	 * Sets consumer for this producer. At the moment of calling this method producer shouldn't have consumer,
	 * as well as consumer shouldn't have producer, otherwise there will be error
	 *
	 * @param consumer consumer for streaming
	 */
	@Override
	public final void setConsumer(StreamConsumer<T> consumer) {
		checkNotNull(consumer);
		checkState(this.consumer == null);
		checkState(getCapabilities().contains(LATE_BINDING) || eventloop.getTick() == createTick,
				LATE_BINDING_ERROR_MESSAGE, this);
		this.consumer = consumer;
		onWired();
		consumer.getEndOfStream()
//				.then(NextStage.onResult(this::endOfStream))
				.then(NextStage.onError(this::closeWithError));
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
		lastDataReceiver.onData(item);
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
		if (logger.isTraceEnabled()) logger.trace("Start producing: {}", this);
		assert dataReceiver != null;
		if (currentDataReceiver == dataReceiver)
			return;
		if (status.isClosed())
			return;
		currentDataReceiver = dataReceiver;
		lastDataReceiver = dataReceiver;
		onProduce(dataReceiver);
	}

	protected void onSuspended() {
	}

	@Override
	public final void suspend() {
		if (logger.isTraceEnabled()) logger.trace("Suspend producer: {}", this);
		if (!isReceiverReady())
			return;
		currentDataReceiver = null;
		onSuspended();
	}

	public final void sendEndOfStream() {
		if (status.isClosed())
			return;
		status = END_OF_STREAM;
		currentDataReceiver = null;
		lastDataReceiver = item -> {};
		eventloop.post(this::cleanup);
		endOfStream.set(null);
	}

	public final void closeWithError(Throwable e) {
		if (status.isClosed())
			return;
		status = CLOSED_WITH_ERROR;
		currentDataReceiver = null;
		lastDataReceiver = item -> {};
		exception = e;
		if (!(e instanceof ExpectedException)) {
			if (logger.isWarnEnabled()) {
				logger.warn("StreamProducer {} closed with error {}", this, exception.toString());
			}
		}
		onError(e);
		eventloop.post(this::cleanup);
		endOfStream.setException(e);
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
	public final Stage<Void> getEndOfStream() {
		return endOfStream;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return emptySet();
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
