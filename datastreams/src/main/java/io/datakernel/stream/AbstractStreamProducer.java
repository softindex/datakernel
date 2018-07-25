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
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
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
	private final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final long createTick = eventloop.tick();

	@Nullable
	private StreamConsumer<T> consumer;

	private StreamStatus status = OPEN;
	private Throwable exception;

	private final SettableStage<Void> endOfStream = new SettableStage<>();

	@Nullable
	private StreamDataReceiver<T> currentDataReceiver;

	private StreamDataReceiver<T> lastDataReceiver = $ -> {
		throw new IllegalStateException("Uninitialized data receiver");
	};

	private final AsyncProduceController controller = new AsyncProduceController();

	private enum ProduceStatus {
		POSTED,
		STARTED,
		STARTED_ASYNC
	}

	private ProduceStatus produceStatus;

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

		checkState(getCapabilities().contains(LATE_BINDING) || eventloop.tick() == createTick,
				LATE_BINDING_ERROR_MESSAGE, this);
		this.consumer = consumer;
		onWired();
		consumer.getEndOfStream()
//				.thenRun(this::endOfStream)
				.whenException(this::closeWithError);
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

	protected void send(T item) {
		getLastDataReceiver().onData(item);
	}

	@Nullable
	public final StreamDataReceiver<T> getCurrentDataReceiver() {
		return currentDataReceiver;
	}

	@Nullable
	public StreamDataReceiver<T> getLastDataReceiver() {
		return lastDataReceiver;
	}

	protected final class AsyncProduceController {
		private AsyncProduceController() {
		}

		public void begin() {
			produceStatus = ProduceStatus.STARTED_ASYNC;
		}

		public void end() {
			produceStatus = null;
		}

		public void resume() {
			if (isReceiverReady()) {
				produce(this);
			} else {
				end();
			}
		}
	}

	protected void produce(AsyncProduceController async) {
	}

	public final void tryProduce() {
		if (!isReceiverReady())
			return;
		if (produceStatus != null)
			return;
		produceStatus = ProduceStatus.STARTED;
		produce(controller);
		if (produceStatus == ProduceStatus.STARTED) {
			produceStatus = null;
		}
	}

	protected void onProduce(StreamDataReceiver<T> dataReceiver) {
		if (produceStatus != null)
			return; // recursive call from downstream - just hot-switch to another receiver
		produceStatus = ProduceStatus.POSTED;
		eventloop.post(() -> {
			produceStatus = null;
			tryProduce();
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

	public void sendEndOfStream() {
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

	@Nullable
	public final Throwable getException() {
		return exception;
	}

	@Override
	public final Stage<Void> getEndOfStream() {
		return endOfStream;
	}

	/**
	 * This method is useful for stream transformers that might add some capability to the stream
	 */
	protected static Set<StreamCapability> addCapabilities(@Nullable StreamProducer<?> producer,
			StreamCapability capability, StreamCapability... capabilities) {
		EnumSet<StreamCapability> result = EnumSet.of(capability, capabilities);
		if (producer != null) {
			result.addAll(producer.getCapabilities());
		}
		return result;
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
