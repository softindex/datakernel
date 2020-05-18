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

package io.datakernel.csp.process;

import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.async.process.AsyncProcess;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.common.api.Recyclable.tryRecycle;

/**
 * An abstract AsyncProcess which describes interactions
 * between ChannelSupplier and ChannelConsumer. A universal
 * class which can be set up for various behaviours. May contain
 * an input ({@link ChannelSupplier}) and output ({@link ChannelConsumer}).
 * <p>
 * After process completes, a {@code Promise} of {@code null} is returned.
 * New process can't be started before the previous one ends.
 * Process can be cancelled or closed manually.
 */
public abstract class AbstractCommunicatingProcess implements AsyncProcess {
	public static final StacklessException ASYNC_PROCESS_IS_COMPLETE = new StacklessException(AbstractCommunicatingProcess.class, "AsyncProcess is complete");

	private boolean processStarted;
	private boolean processComplete;
	private final SettablePromise<Void> processCompletion = new SettablePromise<>();

	protected void beforeProcess() {
	}

	protected void afterProcess(@Nullable Throwable e) {
	}

	public boolean isProcessStarted() {
		return processStarted;
	}

	public boolean isProcessComplete() {
		return processComplete;
	}

	protected void completeProcess() {
		completeProcessEx(null);
	}

	protected void completeProcessEx(@Nullable Throwable e) {
		if (isProcessComplete()) return;
		processComplete = true;
		if (e == null) {
			processCompletion.trySet(null);
			afterProcess(null);
		} else {
			closeEx(e);
		}
	}

	@NotNull
	@Override
	public Promise<Void> getProcessCompletion() {
		return processCompletion;
	}

	/**
	 * Starts this communicating process if it is not started yet.
	 * Consistently executes {@link #beforeProcess()} and
	 * {@link #doProcess()}.
	 *
	 * @return {@code promise} with null result as the marker
	 * of completion of the process
	 */
	@NotNull
	@Override
	public final Promise<Void> startProcess() {
		if (!processStarted) {
			processStarted = true;
			beforeProcess();
			doProcess();
		}
		return processCompletion;
	}

	/**
	 * Describes the main operations of the communicating process.
	 * May include interaction between input ({@link ChannelSupplier})
	 * and output ({@link ChannelConsumer}).
	 */
	protected abstract void doProcess();

	/**
	 * Closes this process if it is not completed yet.
	 * Executes {@link #doClose(Throwable)} and
	 * {@link #afterProcess(Throwable)}.
	 *
	 * @param e exception that is used to close process with
	 */
	@Override
	public final void closeEx(@NotNull Throwable e) {
		if (isProcessComplete()) return;
		processComplete = true;
		doClose(e);
		processCompletion.trySetException(e);
		afterProcess(e);
	}

	/**
	 * An operation which is executed in case
	 * of manual closing.
	 *
	 * @param e an exception thrown on closing
	 */
	protected abstract void doClose(Throwable e);

	/**
	 * Closes this process with {@link AsyncCloseable#CLOSE_EXCEPTION}
	 */
	@Override
	public final void close() {
		AsyncProcess.super.close();
	}

	protected final <T> ChannelSupplier<T> sanitize(ChannelSupplier<T> supplier) {
		return new AbstractChannelSupplier<T>() {
			@Override
			protected Promise<T> doGet() {
				return sanitize(supplier.get());
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				supplier.closeEx(e);
				AbstractCommunicatingProcess.this.closeEx(e);
			}
		};
	}

	protected final <T> ChannelConsumer<T> sanitize(ChannelConsumer<T> consumer) {
		return new AbstractChannelConsumer<T>() {
			@Override
			protected Promise<Void> doAccept(@Nullable T item) {
				return sanitize(consumer.accept(item));
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				consumer.closeEx(e);
				AbstractCommunicatingProcess.this.closeEx(e);
			}
		};
	}

	protected final BinaryChannelSupplier sanitize(BinaryChannelSupplier supplier) {
		return new BinaryChannelSupplier(supplier.getBufs()) {
			@Override
			public Promise<Void> needMoreData() {
				return sanitize(supplier.needMoreData());
			}

			@Override
			public Promise<Void> endOfStream() {
				return sanitize(supplier.endOfStream());
			}

			@Override
			public void closeEx(@NotNull Throwable e) {
				supplier.closeEx(e);
				AbstractCommunicatingProcess.this.closeEx(e);
			}
		};
	}

	protected final <T> Promise<T> sanitize(Promise<T> promise) {
		return promise.async()
				.thenEx(this::sanitize);
	}

	/**
	 * Closes this process and returns a promise of {@code e}
	 * exception if provided {@code e} is not {@code null}.
	 * Otherwise, returns a promise of {@code value}. If the
	 * process was already completed, returns a promise of
	 * {@link #ASYNC_PROCESS_IS_COMPLETE} and recycles the
	 * provided {@code value}.
	 *
	 * @return a promise of {@code value} if {@code e} is
	 * {@code null} and {@code promise} of {@code e} exception
	 * otherwise. If the process was already completed,
	 * returns {@link #ASYNC_PROCESS_IS_COMPLETE}.
	 */
	protected final <T> Promise<T> sanitize(T value, @Nullable Throwable e) {
		if (isProcessComplete()) {
			tryRecycle(value);
			if (value instanceof AsyncCloseable) {
				((AsyncCloseable) value).closeEx(ASYNC_PROCESS_IS_COMPLETE);
			}
			return Promise.ofException(ASYNC_PROCESS_IS_COMPLETE);
		}
		if (e == null) {
			return Promise.of(value);
		} else {
			closeEx(e);
			return Promise.ofException(e);
		}
	}

}
