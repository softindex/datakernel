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
	private final ArrayDeque<T> buffer = new ArrayDeque<>();

	{
		dataAcceptorSafe = buffer::addLast;
	}

	private StreamConsumer<T> consumer;

	@SuppressWarnings("FieldCanBeLocal")
	private boolean flushRequest;
	@Nullable
	private FlushStatus flushStatus;

	private enum FlushStatus {
		STARTED,
		STARTED_ASYNC
	}

	private boolean endOfStreamRequest;
	private SettablePromise<Void> endOfStream = new SettablePromise<>();

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();

	@Override
	public Promise<Void> streamTo(@NotNull StreamConsumer<T> consumer) {
		checkState(eventloop.inEventloopThread());
		checkState(!isStarted());
		this.consumer = consumer;
		if (!this.getEndOfStream().isException() && !consumer.getAcknowledgement().isException()) {
			onStarted();
		}
		return StreamSupplier.super.streamTo(consumer);
	}

	protected void onStarted() {
	}

	public final boolean isStarted() {
		return consumer != null;
	}

	public final StreamConsumer<T> getConsumer() {
		return consumer;
	}

	@Override
	public final void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		checkState(eventloop.inEventloopThread());
		if (endOfStream.isComplete()) return;
		if (this.dataAcceptor == dataAcceptor) return;
		this.dataAcceptor = dataAcceptor;
		this.dataAcceptorSafe = dataAcceptor != null ? dataAcceptor : buffer::addLast;
		if (isReady()) {
			flush();
		} else {
			onSuspended();
		}
	}

	protected final void asyncBegin() {
		checkState(flushStatus == FlushStatus.STARTED || flushStatus == FlushStatus.STARTED_ASYNC);
		flushStatus = FlushStatus.STARTED_ASYNC;
	}

	protected final void asyncEnd() {
		checkState(flushStatus == FlushStatus.STARTED_ASYNC);
		flushStatus = null;
	}

	protected final void asyncResume() {
		checkState(flushStatus == FlushStatus.STARTED_ASYNC);
		if (isReady()) {
			onResumed();
		} else {
			asyncEnd();
		}
	}

	/**
	 * Causes this supplier to try to supply its buffered items and updates the current state accordingly.
	 */
	public final void flush() {
		checkState(eventloop.inEventloopThread());
		flushRequest = true;
		if (flushStatus != null) return; // recursive call
		if (endOfStream.isComplete()) return;
		flushStatus = FlushStatus.STARTED;
		while (flushRequest) {
			flushRequest = false;
			while (isReady() && !buffer.isEmpty()) {
				T item = buffer.pollFirst();
				this.dataAcceptor.accept(item);
			}
			if (isReady() && !endOfStreamRequest) {
				onResumed();
			}
		}

		if (endOfStreamRequest && buffer.isEmpty()) {
			dataAcceptor = null;
			//noinspection unchecked
			dataAcceptorSafe = (StreamDataAcceptor<T>) NO_ACCEPTOR;
			if (endOfStream.trySet(null)) {
				cleanup();
			}
			return;
		}

		if (flushStatus == FlushStatus.STARTED) {
			flushStatus = null;
		}
	}

	/**
	 * Called when this supplier changes from suspended state to a normal one.
	 */
	protected abstract void onResumed();

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
	 * This operation is final and cannot be undone.
	 * Only the first call causes any effect.
	 */
	public final void sendEndOfStream() {
		checkState(eventloop.inEventloopThread());
		if (endOfStream.isComplete()) return;
		if (flushStatus == FlushStatus.STARTED_ASYNC) {
			asyncEnd();
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

	public final boolean isClosed() {
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
			cleanup();
		}
	}

	/**
	 * This method will be called when this supplier erroneously changes to the closed state.
	 */
	protected void onError(Throwable e) {
	}

	private void cleanup() {
		onComplete();
		eventloop.post(this::onCleanup);
		buffer.clear();
		SettablePromise<Void> endOfStream = this.endOfStream;
		this.endOfStream = new SettablePromise<>();
		assert endOfStream.isComplete();
		endOfStream.whenComplete(this.endOfStream);
	}

	protected void onComplete() {
	}

	/**
	 * This method will be asynchronously called after this supplier changes to the closed state regardless of error.
	 */
	protected void onCleanup() {
	}
}
