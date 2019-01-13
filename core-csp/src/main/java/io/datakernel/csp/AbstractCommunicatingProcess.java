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

package io.datakernel.csp;

import io.datakernel.async.*;
import io.datakernel.csp.binary.BinaryChannelSupplier;
import io.datakernel.exception.ConstantException;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.util.Recyclable.tryRecycle;

public abstract class AbstractCommunicatingProcess implements AsyncProcess {
	public static final ConstantException ASYNC_PROCESS_IS_COMPLETE = new ConstantException(AbstractCommunicatingProcess.class, "AsyncProcess is complete");

	private boolean processStarted;
	private boolean processComplete;
	private SettablePromise<Void> processResult = new SettablePromise<>();

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
		completeProcess(null);
	}

	protected void completeProcess(@Nullable Throwable e) {
		if (isProcessComplete()) return;
		processComplete = true;
		if (e == null) {
			processResult.trySet(null);
			afterProcess(null);
		} else {
			close(e);
		}
	}

	@Override
	public MaterializedPromise<Void> getProcessResult() {
		return processResult;
	}

	@Override
	public final MaterializedPromise<Void> startProcess() {
		if (!processStarted) {
			processStarted = true;
			beforeProcess();
			doProcess();
		}
		return processResult;
	}

	protected abstract void doProcess();

	@Override
	public final void close(Throwable e) {
		if (isProcessComplete()) return;
		processComplete = true;
		doClose(e);
		processResult.trySetException(e);
		afterProcess(e);
	}

	protected abstract void doClose(Throwable e);

	@Override
	public final void cancel() {
		AsyncProcess.super.cancel();
	}

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
			protected void onClosed(Throwable e) {
				supplier.close(e);
				AbstractCommunicatingProcess.this.close(e);
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
			protected void onClosed(Throwable e) {
				consumer.close(e);
				AbstractCommunicatingProcess.this.close(e);
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
			public void close(Throwable e) {
				supplier.close(e);
				AbstractCommunicatingProcess.this.close(e);
			}
		};
	}

	protected final <T> Promise<T> sanitize(Promise<T> promise) {
		assert !isProcessComplete();
		return promise
				.thenComposeEx(this::sanitize);
	}

	public <T> Promise<T> sanitize(T value, Throwable e) {
		if (isProcessComplete()) {
			tryRecycle(value);
			if (value instanceof Cancellable) {
				((Cancellable) value).close(ASYNC_PROCESS_IS_COMPLETE);
			}
			return Promise.ofException(ASYNC_PROCESS_IS_COMPLETE);
		}
		if (e == null) {
			return Promise.of(value);
		} else {
			close(e);
			return Promise.ofException(e);
		}
	}

}
