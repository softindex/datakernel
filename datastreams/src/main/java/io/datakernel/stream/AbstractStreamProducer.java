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
import io.datakernel.async.MaterializedStage;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.util.Recyclable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Set;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
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

	@Nullable
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
		consumer.getAcknowledgement()
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

	public final void postProduce() {
		if (produceStatus != null)
			return; // recursive call from downstream - just hot-switch to another receiver
		produceStatus = ProduceStatus.POSTED;
		eventloop.post(() -> {
			produceStatus = null;
			tryProduce();
		});
	}

	protected void onProduce(StreamDataReceiver<T> dataReceiver) {
		postProduce();
	}

	@Override
	public final void produce(StreamDataReceiver<T> dataReceiver) {
		if (logger.isTraceEnabled()) logger.trace("Start producing: {}", this);
		assert dataReceiver != null;

		if (currentDataReceiver == dataReceiver) return;
		if (endOfStream.isComplete()) return;
		currentDataReceiver = dataReceiver;
		lastDataReceiver = dataReceiver;
		onProduce(dataReceiver);
	}

	protected boolean isClosed() {
		return endOfStream.isComplete();
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

	public Stage<Void> sendEndOfStream() {
		if (endOfStream.isComplete()) return endOfStream;
		currentDataReceiver = null;
		lastDataReceiver = Recyclable::deepRecycle;
		endOfStream.set(null);
		eventloop.post(this::cleanup);
		return consumer.getAcknowledgement();
	}

	@Override
	public final void closeWithError(Throwable e) {
		if (endOfStream.isComplete()) return;
		if (!(e instanceof ExpectedException)) {
			if (logger.isWarnEnabled()) {
				logger.warn("StreamProducer {} closed with error {}", this, e.toString());
			}
		}
		currentDataReceiver = null;
		lastDataReceiver = Recyclable::deepRecycle;
		endOfStream.setException(e);
		eventloop.post(this::cleanup);
		onError(e);
	}

	protected abstract void onError(Throwable t);

	protected void cleanup() {
	}

	@Override
	public final MaterializedStage<Void> getEndOfStream() {
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
