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

import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.common.Preconditions.checkState;

/**
 * This is a helper partial implementation of the {@link StreamConsumer}
 * which helps to deal with state transitions and helps to implement basic behaviours.
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();
	protected final SettablePromise<Void> acknowledgement = new SettablePromise<>();
	private StreamSupplier<T> streamSupplier;
	private boolean endOfStream;

	{
		acknowledgement.async().whenComplete(this::onCleanup);
	}

	@Override
	public final void consume(@NotNull StreamSupplier<T> streamSupplier) {
		//noinspection ResultOfMethodCallIgnored
		checkNotNull(streamSupplier);
		checkState(!acknowledgement.isComplete());
		checkState(!endOfStream);
		checkState(this.streamSupplier == null);
		this.streamSupplier = streamSupplier;
		streamSupplier.getEndOfStream()
				.whenResult(this::endOfStream)
				.whenException(this::closeEx);
		if (!getAcknowledgement().isComplete()) {
			onStarted();
		}
	}

	/**
	 * This method will be called when this consumer begins receiving items.
	 * It may not be called if consumer never received anything getting closed.
	 */
	protected void onStarted() {
	}

	private void endOfStream() {
		checkState(!acknowledgement.isComplete());
		checkState(!endOfStream);
		endOfStream = true;
		onEndOfStream();
	}

	/**
	 * This method will be called when associated supplier closes.
	 */
	protected void onEndOfStream() {
	}

	/**
	 * Returns <code>true</code> when associated supplier gets closed,
	 * this may happen before {@link #getAcknowledgement() acknowledgement}.
	 */
	public boolean isEndOfStream() {
		return endOfStream;
	}

	/**
	 * Begins receiving data into given acceptor, resumes the associated supplier to receive data from it.
	 */
	public final void resume(@NotNull StreamDataAcceptor<T> dataAcceptor) {
		//noinspection ResultOfMethodCallIgnored
		checkNotNull(dataAcceptor);
		if (acknowledgement.isComplete()) return;
		if (endOfStream) return;
		if (streamSupplier == null) return;
		streamSupplier.resume(dataAcceptor);
	}

	/**
	 * Suspends the associated supplier.
	 */
	public final void suspend() {
		if (acknowledgement.isComplete()) return;
		if (endOfStream) return;
		streamSupplier.suspend();
	}

	/**
	 * Triggers the {@link #getAcknowledgement() acknowledgement} of this consumer.
	 */
	public final void acknowledge() {
		if (acknowledgement.isComplete()) return;
		acknowledgement.set(null);
	}

	@Override
	public final Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	/**
	 * Unlike {@link #isEndOfStream()}, returns <code>true</code> when this consumer
	 * has its {@link #getAcknowledgement() acknowledgement} set, meaning that it is completely closed.
	 */
	public boolean isClosed() {
		return acknowledgement.isComplete();
	}

	@Override
	public final void closeEx(@NotNull Throwable e) {
		//noinspection ResultOfMethodCallIgnored
		checkNotNull(e);
		if (acknowledgement.trySetException(e)) {
			onError(e);
		}
	}

	/**
	 * This method will be called when this consumer erroneously changes to the acknowledged state.
	 */
	protected void onError(Throwable e) {
	}

	/**
	 * This method will be asynchronously called after this consumer changes to the acknowledged state regardless of error.
	 */
	protected void onCleanup() {
	}
}
