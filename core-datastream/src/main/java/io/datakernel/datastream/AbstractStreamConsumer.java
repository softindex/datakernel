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

package io.datakernel.datastream;

import io.datakernel.common.exception.ExpectedException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Set;

import static io.datakernel.common.Preconditions.checkState;
import static io.datakernel.datastream.StreamCapability.LATE_BINDING;
import static java.util.Collections.emptySet;

/**
 * It is basic implementation of {@link StreamConsumer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final long createTick = eventloop.tick();

	private StreamSupplier<T> supplier;

	private final SettablePromise<Void> endOfStream = new SettablePromise<>();
	private final SettablePromise<Void> acknowledgement = new SettablePromise<>();

	/**
	 * Sets wired supplier. It will sent data to this consumer
	 *
	 * @param supplier stream supplier for setting
	 */

	@Override
	public final void setSupplier(@NotNull StreamSupplier<T> supplier) {
		checkState(this.supplier == null, "Supplier has already been set");
		checkState(getCapabilities().contains(LATE_BINDING) || eventloop.tick() == createTick,
				LATE_BINDING_ERROR_MESSAGE, this);
		this.supplier = supplier;
		onWired();
		supplier.getEndOfStream()
				.whenComplete(endOfStream)
				.whenException(this::close)
				.post()
				.whenResult($1 -> onEndOfStream()
						.whenException(this::close)
						.post()
						.whenResult($2 -> acknowledge()));
	}

	protected void onWired() {
		eventloop.post(this::onStarted);
	}

	protected void onStarted() {
	}

	public boolean isWired() {
		return supplier != null;
	}

	public final StreamSupplier<T> getSupplier() {
		return supplier;
	}

	protected final void acknowledge() {
		if (acknowledgement.isComplete()) return;
		acknowledgement.set(null);
		eventloop.post(this::cleanup);
	}

	protected abstract Promise<Void> onEndOfStream();

	@Override
	public final void close(@NotNull Throwable e) {
		if (acknowledgement.isComplete()) return;
		acknowledgement.setException(e);
		if (!(e instanceof ExpectedException)) {
			if (logger.isWarnEnabled()) {
				logger.warn("StreamConsumer {} closed with error {}", this, e.toString());
			}
		}
		onError(e);
		eventloop.post(this::cleanup);
	}

	protected abstract void onError(Throwable e);

	protected void cleanup() {
	}

	public Promise<Void> getEndOfStream() {
		return endOfStream;
	}

	@Override
	public final Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	/**
	 * This method is useful for stream transformers that might add some capability to the stream
	 */
	protected static Set<StreamCapability> extendCapabilities(@Nullable StreamConsumer<?> consumer,
			StreamCapability capability, StreamCapability... capabilities) {
		EnumSet<StreamCapability> result = EnumSet.of(capability, capabilities);
		if (consumer != null) {
			result.addAll(consumer.getCapabilities());
		}
		return result;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return emptySet();
	}

}
