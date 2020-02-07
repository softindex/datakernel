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

import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

import static io.datakernel.common.Preconditions.checkState;

/**
 * It is basic implementation of {@link StreamSupplier}
 *
 * @param <T> type of received item
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

	private boolean endOfStreamRequest;
	private final SettablePromise<Void> endOfStream = new SettablePromise<>();

	{
		endOfStream.async().whenComplete(this::onCleanup);
	}

	@SuppressWarnings("FieldCanBeLocal")
	private boolean produceRequest;

	private final AsyncProduceController controller = new AsyncProduceController();

	@Nullable
	private ProduceStatus produceStatus;

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
	public final void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		checkState(!endOfStream.isComplete());
		if (this.dataAcceptor == dataAcceptor) return;
		this.dataAcceptor = dataAcceptor;
		this.dataAcceptorSafe = dataAcceptor != null ? dataAcceptor : buffer::addLast;
		flush();
	}

	public final void flush() {
		if (endOfStream.isComplete()) return;
		produceRequest = true;
		if (produceStatus != null) return; // recursive call
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
				} else {
					dataAcceptor = null;
					//noinspection unchecked
					dataAcceptorSafe = (StreamDataAcceptor<T>) NO_ACCEPTOR;
					endOfStream.set(null);
					return;
				}
			}
		}
		if (produceStatus == ProduceStatus.STARTED) {
			produceStatus = null;
		}
		if (this.dataAcceptor == null) {
			onSuspended();
		}
	}

	protected abstract void onResumed(AsyncProduceController async);

	protected void onSuspended() {
	}

	public final void send(T item) {
		dataAcceptorSafe.accept(item);
	}

	public final void sendEndOfStream() {
		if (endOfStream.isComplete()) return;
		if (produceStatus == ProduceStatus.STARTED_ASYNC) {
			produceStatus = null;
		}
		endOfStreamRequest = true;
		flush();
	}

	@Nullable
	public final StreamDataAcceptor<T> getDataAcceptor() {
		return dataAcceptor;
	}

	public final boolean isReady() {
		return dataAcceptor != null;
	}

	@Override
	public final Promise<Void> getEndOfStream() {
		return endOfStream;
	}

	@Override
	public final void closeEx(@NotNull Throwable e) {
		dataAcceptor = null;
		//noinspection unchecked
		dataAcceptorSafe = (StreamDataAcceptor<T>) NO_ACCEPTOR;
		if (endOfStream.trySetException(e)) {
			onError(e);
		}
	}

	protected void onError(Throwable e) {
	}

	protected void onCleanup() {
	}

}
