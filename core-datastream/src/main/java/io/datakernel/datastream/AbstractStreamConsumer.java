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
 * It is basic implementation of {@link StreamSupplier}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();
	protected final SettablePromise<Void> acknowledgement = new SettablePromise<>();
	private StreamDataSource<T> dataSource;
	private boolean endOfStream;

	{
		acknowledgement.async().whenComplete(this::onCleanup);
	}

	@Override
	public final void consume(@NotNull StreamDataSource<T> dataSource) {
		//noinspection ResultOfMethodCallIgnored
		checkNotNull(dataSource);
		checkState(!acknowledgement.isComplete());
		checkState(!endOfStream);
		checkState(this.dataSource == null);
		this.dataSource = dataSource;
		onStarted();
	}

	protected void onStarted() {
	}

	@Override
	public final void endOfStream() {
		checkState(!acknowledgement.isComplete());
		checkState(!endOfStream);
		endOfStream = true;
		onEndOfStream();
	}

	protected void onEndOfStream() {
	}

	public boolean isEndOfStream() {
		return endOfStream;
	}

	public final void resume(@NotNull StreamDataAcceptor<T> dataAcceptor) {
		//noinspection ResultOfMethodCallIgnored
		checkNotNull(dataAcceptor);
		if (acknowledgement.isComplete()) return;
		if (endOfStream) return;
		if (dataSource == null) return;
		dataSource.resume(dataAcceptor);
	}

	public final void suspend() {
		if (acknowledgement.isComplete()) return;
		if (endOfStream) return;
		dataSource.suspend();
	}

	public final void acknowledge() {
		if (acknowledgement.isComplete()) return;
		acknowledgement.set(null);
	}

	@Override
	public final Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	public boolean isClosed() {
		return acknowledgement.isComplete();
	}

	@Override
	public final void close(@NotNull Throwable e) {
		//noinspection ResultOfMethodCallIgnored
		checkNotNull(e);
		if (acknowledgement.trySetException(e)) {
			onError(e);
		}
	}

	protected void onError(Throwable e) {
	}

	protected void onCleanup() {
	}
}
