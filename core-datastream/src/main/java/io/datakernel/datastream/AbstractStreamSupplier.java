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

import io.datakernel.common.Check;
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
	private final boolean CHECK = Check.isEnabled(getClass());

	public static final StreamDataAcceptor<?> NO_ACCEPTOR = item -> {};

	@Nullable
	private StreamDataAcceptor<T> dataAcceptor;
	private StreamDataAcceptor<T> dataAcceptorSafe;
	private final ArrayDeque<T> buffer = new ArrayDeque<>();

	private StreamConsumer<T> consumer;

	private boolean flushRequest;
	private boolean flushRunning;
	private boolean initialized;
	private int flushAsync;

	private boolean endOfStreamRequest;
	private final SettablePromise<Void> endOfStream = new SettablePromise<>();

	@Nullable
	private SettablePromise<Void> flushPromise;

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();

	{
		dataAcceptorSafe = buffer::addLast;
		if (eventloop.inEventloopThread()){
			eventloop.post(this::tryInitialize);
		} else {
			eventloop.execute(this::tryInitialize);
		}
	}

	@Override
	public final Promise<Void> streamTo(@NotNull StreamConsumer<T> consumer) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		checkState(!isStarted());
		this.consumer = consumer;
		consumer.getAcknowledgement()
				.whenResult(this::acknowledge)
				.whenException(this::closeEx);
		tryInitialize();
		if (!isEndOfStream()) {
			onStarted();
		}
		//noinspection unchecked
		this.dataAcceptor = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		consumer.consume(this);
		updateDataAcceptor();
		return consumer.getAcknowledgement();
	}

	/**
	 * This method will be called exactly once: either in the next eventloop tick after creation of this supplier
	 * or right before {@link #onStarted()} or {@link #onError(Throwable)} calls
	 */
	protected void onInit() {
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
	public final void updateDataAcceptor() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (!isStarted()) return;
		if (endOfStream.isComplete()) return;
		StreamDataAcceptor<T> dataAcceptor = this.consumer.getDataAcceptor();
		if (this.dataAcceptor == dataAcceptor) return;
		this.dataAcceptor = dataAcceptor;
		if (dataAcceptor != null) {
			if (!isEndOfStream()) {
				this.dataAcceptorSafe = dataAcceptor;
			}
			flush();
		} else if (!isEndOfStream()) {
			this.dataAcceptorSafe = buffer::addLast;
			onSuspended();
		}
	}

	protected final void asyncBegin() {
		flushAsync++;
	}

	protected final void asyncEnd() {
		checkState(flushAsync > 0);
		flushAsync--;
	}

	protected final void asyncResume() {
		checkState(flushAsync > 0);
		flushAsync--;
		resume();
	}

	protected final void resume() {
		if (flushRunning) {
			flushRequest = true;
		} else if (isReady() && !isEndOfStream()) {
			onResumed();
		}
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
	public final Promise<Void> sendEndOfStream() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (endOfStreamRequest) return flushPromise;
		if (flushAsync > 0) {
			asyncEnd();
		}
		endOfStreamRequest = true;
		//noinspection unchecked
		this.dataAcceptorSafe = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		flush();
		return getFlushPromise();
	}

	/**
	 * Returns a promise that will be completed when all data items are propagated
	 * to the actual data acceptor
	 */
	@NotNull
	public final Promise<Void> getFlushPromise() {
		if (isEndOfStream()) {
			return endOfStream;
		} else if (flushPromise != null) {
			return flushPromise;
		} else if (dataAcceptor != null) {
			return Promise.complete();
		} else {
			flushPromise = new SettablePromise<>();
			return flushPromise;
		}
	}

	/**
	 * Initializes this supplier by calling {@link #onInit()} only if it has not already been initialized.
	 */
	private void tryInitialize() {
		if (!initialized) {
			initialized = true;
			onInit();
		}
	}

	/**
	 * Causes this supplier to try to supply its buffered items and updates the current state accordingly.
	 */
	private void flush() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		flushRequest = true;
		if (flushRunning || flushAsync > 0) return; // recursive call
		if (endOfStream.isComplete()) return;
		if (!isStarted()) return;

		flushRunning = true;
		while (flushRequest) {
			flushRequest = false;
			while (isReady() && !buffer.isEmpty()) {
				T item = buffer.pollFirst();
				this.dataAcceptor.accept(item);
			}
			if (isReady() && !isEndOfStream()) {
				onResumed();
			}
		}
		flushRunning = false;

		if (flushAsync > 0) return;
		if (!buffer.isEmpty()) return;
		if (endOfStream.isComplete()) return;

		if (!endOfStreamRequest) {
			if (this.flushPromise != null) {
				SettablePromise<Void> flushPromise = this.flushPromise;
				this.flushPromise = null;
				flushPromise.set(null);
			}
			return;
		}

		dataAcceptor = null;
		if (flushPromise != null) {
			flushPromise.set(null);
		}
		endOfStream.set(null);
		cleanup();
	}

	/**
	 * Called when this supplier changes from suspended state to a normal one.
	 */
	protected void onResumed() {
	}

	/**
	 * Called when this supplier changes a normal state to a suspended one.
	 */
	protected void onSuspended() {
	}

	/**
	 * Returns current data acceptor (the last one set with the {@link #updateDataAcceptor()} method)
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
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		return endOfStream;
	}

	public final boolean isEndOfStream() {
		return endOfStreamRequest;
	}

	private void acknowledge() {
		tryInitialize();
		onAcknowledge();
		close();
	}

	protected void onAcknowledge() {
	}

	@Override
	public final void closeEx(@NotNull Throwable e) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		endOfStreamRequest = true;
		dataAcceptor = null;
		//noinspection unchecked
		dataAcceptorSafe = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		if (flushPromise != null) {
			flushPromise.trySetException(e);
		}
		if (endOfStream.trySetException(e)) {
			tryInitialize();
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
		endOfStream.resetCallbacks();
		if (flushPromise != null) {
			flushPromise.resetCallbacks();
		}
	}

	protected void onComplete() {
	}

	/**
	 * This method will be asynchronously called after this supplier changes to the closed state regardless of error.
	 */
	protected void onCleanup() {
	}
}
