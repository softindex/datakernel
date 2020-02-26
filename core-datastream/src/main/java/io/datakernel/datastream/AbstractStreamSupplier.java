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

package io.datakernel.datastream;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.datakernel.common.Preconditions.checkState;

/**
 * This is a helper partial implementation of the {@link StreamSupplier}
 * which helps to deal with state transitions and helps to implement basic behaviours.
 */
public abstract class AbstractStreamSupplier<T> implements StreamSupplier<T> {
	public static final StreamDataAcceptor<?> NO_ACCEPTOR = item -> {};

	@Nullable
	private StreamDataAcceptor<T> dataAcceptor;
	private StreamDataAcceptor<T> dataAcceptorSafe;

	@SuppressWarnings("FieldCanBeLocal")
	private boolean produceRequest;
	@Nullable
	private ProduceStatus produceStatus;

	private final ArrayDeque<T> buffer = new ArrayDeque<>();

	{
		dataAcceptorSafe = buffer::addLast;
	}

	private boolean endOfStreamRequest;
	private final SettablePromise<Void> endOfStream = new SettablePromise<>();

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();

	{
		endOfStream.async().whenComplete(this::onCleanup);
	}

	private final AsyncProduceController controller = new AsyncProduceController();

	private enum ProduceStatus {
		STARTED,
		STARTED_ASYNC
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
			if (isReady()) {
				onResumed(this);
			} else {
				end();
			}
		}
	}

	@Override
	public final void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		checkState(eventloop.inEventloopThread());
		if (endOfStream.isComplete()) return;
		if (this.dataAcceptor == dataAcceptor) return;
		this.dataAcceptor = dataAcceptor;
		this.dataAcceptorSafe = dataAcceptor != null ? dataAcceptor : buffer::addLast;
		if (this.dataAcceptor != null) {
			flush();
		} else {
			onSuspended();
		}
	}

	/**
	 * Causes this supplier to try to supply its buffered items and updates the current state accordingly.
	 */
	public final void flush() {
		checkState(eventloop.inEventloopThread());
		produceRequest = true;
		if (produceStatus != null) return; // recursive call
		if (endOfStream.isComplete()) return;
		produceStatus = ProduceStatus.STARTED;
		while (produceRequest) {
			produceRequest = false;
			while (this.dataAcceptor != null && !buffer.isEmpty()) {
				T item = buffer.pollFirst();
				this.dataAcceptor.accept(item);
			}
			if (this.dataAcceptor != null) {
				//noinspection ConstantConditions
				assert buffer.isEmpty();
				if (!endOfStreamRequest) {
					onResumed(controller);
				}
			}
		}

		if (endOfStreamRequest && buffer.isEmpty()) {
			dataAcceptor = null;
			//noinspection unchecked
			dataAcceptorSafe = (StreamDataAcceptor<T>) NO_ACCEPTOR;
			endOfStream.trySet(null);
			return;
		}

		if (produceStatus == ProduceStatus.STARTED) {
			produceStatus = null;
		}
	}

	/**
	 * Called when this supplier changes from suspended state to a normal one.
	 */
	protected abstract void onResumed(AsyncProduceController async);

	/**
	 * Called when this supplier changes a normal state to a suspended one.
	 */
	protected void onSuspended() {
	}

	/**
	 * Sends given item through this supplier.
	 * <p>
	 * This method stores the item to an internal buffer if supplier is in a suspended state,
	 * and must never be called when supplier reaches {@link #sendEndOfStream() end of stream}.
	 */
	public final void send(T item) {
		dataAcceptorSafe.accept(item);
	}

	/**
	 * Puts this supplier in closed state with no error.
	 * This operation is final, cannot be undone and must not be called multiple times.
	 */
	public final void sendEndOfStream() {
		checkState(eventloop.inEventloopThread());
		if (endOfStream.isComplete()) return;
		if (produceStatus == ProduceStatus.STARTED_ASYNC) {
			produceStatus = null;
		}
		endOfStreamRequest = true;
		flush();
	}

	/**
	 * Returns current data acceptor (the last one set with the {@link #resume} method)
	 * or <code>null</code> when this supplier is in a suspended state.
	 */
	@Nullable
	public final StreamDataAcceptor<T> getDataAcceptor() {
		return dataAcceptor;
	}

	/**
	 * Returns <code>true</code> when this supplier is in normal state and
	 * <cod>false</cod> when it is suspended or closed.
	 */
	public final boolean isReady() {
		return dataAcceptor != null;
	}

	@Override
	public final Promise<Void> getEndOfStream() {
		checkState(eventloop.inEventloopThread());
		return endOfStream;
	}

	public boolean isClosed() {
		return endOfStream.isComplete();
	}

	@Override
	public final void closeEx(@NotNull Throwable e) {
		checkState(eventloop.inEventloopThread());
		dataAcceptor = null;
		//noinspection unchecked
		dataAcceptorSafe = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		if (endOfStream.trySetException(e)) {
			onError(e);
		}
	}

	/**
	 * This method will be called when this supplier erroneously changes to the closed state.
	 */
	protected void onError(Throwable e) {
	}

	/**
	 * This method will be asynchronously called after this supplier changes to the closed state regardless of error.
	 */
	protected void onCleanup() {
	}
}
