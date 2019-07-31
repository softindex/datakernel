/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.util.Recyclable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Set;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.emptySet;

/**
 * It is basic implementation of {@link StreamSupplier}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamSupplier<T> implements StreamSupplier<T> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final long createTick = eventloop.tick();

	@Nullable
	private StreamConsumer<T> consumer;

	private final SettablePromise<Void> endOfStream = new SettablePromise<>();
	private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

	@Nullable
	private StreamDataAcceptor<T> currentDataAcceptor;

	private StreamDataAcceptor<T> lastDataAcceptor = $ -> {
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

	/**
	 * Sets consumer for this supplier. At the moment of calling this method supplier shouldn't have consumer,
	 * as well as consumer shouldn't have supplier, otherwise there will be error
	 *
	 * @param consumer consumer for streaming
	 */
	@Override
	public final void setConsumer(StreamConsumer<T> consumer) {
		checkNotNull(consumer);
		checkState(this.consumer == null, "Consumer has already been set");

		checkState(getCapabilities().contains(LATE_BINDING) || eventloop.tick() == createTick,
				LATE_BINDING_ERROR_MESSAGE, this);
		this.consumer = consumer;
		onWired();
		consumer.getAcknowledgement()
				.whenException(this::close)
				.whenComplete(acknowledgement);
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
		return currentDataAcceptor != null;
	}

	protected void send(T item) {
		lastDataAcceptor.accept(item);
	}

	@Nullable
	public final StreamDataAcceptor<T> getCurrentDataAcceptor() {
		return currentDataAcceptor;
	}

	public StreamDataAcceptor<T> getLastDataAcceptor() {
		return lastDataAcceptor;
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

	protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
		postProduce();
	}

	@Override
	public final void resume(StreamDataAcceptor<T> dataAcceptor) {
		if (logger.isTraceEnabled()) logger.trace("Start producing: {}", this);
		assert dataAcceptor != null;

		if (currentDataAcceptor == dataAcceptor) return;
		if (endOfStream.isComplete()) return;
		currentDataAcceptor = dataAcceptor;
		lastDataAcceptor = dataAcceptor;
		onProduce(dataAcceptor);
	}

	protected boolean isClosed() {
		return endOfStream.isComplete();
	}

	protected void onSuspended() {
	}

	@Override
	public final void suspend() {
		if (logger.isTraceEnabled()) logger.trace("Suspend supplier: {}", this);
		if (!isReceiverReady())
			return;
		currentDataAcceptor = null;
		onSuspended();
	}

	public Promise<Void> sendEndOfStream() {
		assert consumer != null;
		if (endOfStream.isComplete()) return endOfStream;
		currentDataAcceptor = null;
		lastDataAcceptor = Recyclable::tryRecycle;
		endOfStream.set(null);
		eventloop.post(this::cleanup);
		return consumer.getAcknowledgement();
	}

	@Override
	public final void close(@NotNull Throwable e) {
		if (endOfStream.isComplete()) return;
		if (!(e instanceof ExpectedException)) {
			if (logger.isWarnEnabled()) {
				logger.warn("StreamSupplier {} closed with error {}", this, e.toString());
			}
		}
		currentDataAcceptor = null;
		lastDataAcceptor = Recyclable::tryRecycle;
		endOfStream.setException(e);
		eventloop.post(this::cleanup);
		onError(e);
	}

	protected abstract void onError(Throwable e);

	protected void cleanup() {
	}

	@Override
	public final Promise<Void> getEndOfStream() {
		return endOfStream;
	}

	public Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	/**
	 * This method is useful for stream transformers that might add some capability to the stream
	 */
	protected static Set<StreamCapability> addCapabilities(@Nullable StreamSupplier<?> supplier,
			StreamCapability capability, StreamCapability... capabilities) {
		EnumSet<StreamCapability> result = EnumSet.of(capability, capabilities);
		if (supplier != null) {
			result.addAll(supplier.getCapabilities());
		}
		return result;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return emptySet();
	}

}
