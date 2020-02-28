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

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

final class StreamSuppliers {

	static final class ClosingWithError<T> implements StreamSupplier<T> {
		private final Promise<Void> endOfStream;

		ClosingWithError(Throwable e) {
			this.endOfStream = Promise.ofException(e);
		}

		@Override
		public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
		}
	}

	static final class Closing<T> implements StreamSupplier<T> {
		@Override
		public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return Promise.complete();
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
		}
	}

	static final class Idle<T> implements StreamSupplier<T> {
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void resume(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void closeEx(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	static final class OfIterator<T> extends AbstractStreamSupplier<T> {
		private final Iterator<T> iterator;

		/**
		 * Creates a new instance of  StreamSupplierOfIterator
		 *
		 * @param iterator iterator with object which need to send
		 */
		public OfIterator(@NotNull Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		protected void onResumed() {
			while (isReady() && iterator.hasNext()) {
				send(iterator.next());
			}
			if (!iterator.hasNext()) {
				sendEndOfStream();
			}
		}
	}

	static final class OfPromise<T> extends AbstractStreamSupplier<T> {
		@Nullable
		private StreamSupplier<T> supplier;

		public OfPromise(Promise<? extends StreamSupplier<T>> promise) {
			promise
					.whenResult(supplier -> {
						supplier.getEndOfStream()
								.whenResult(this::sendEndOfStream)
								.whenException(this::closeEx);
						this.getEndOfStream()
								.whenException(supplier::closeEx);
						if (isClosed()) return;

						this.supplier = supplier;
						this.supplier.resume(getDataAcceptor());
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onResumed() {
			if (supplier != null) {
				supplier.resume(getDataAcceptor());
			}
		}

		@Override
		protected void onSuspended() {
			if (supplier != null) {
				supplier.suspend();
			}
		}

		@Override
		protected void onError(Throwable e) {
			if (supplier != null) {
				supplier.closeEx(e);
			}
		}

		@Override
		protected void onComplete() {
			supplier = null;
		}
	}

	static final class Concat<T> extends AbstractStreamSupplier<T> {
		private ChannelSupplier<StreamSupplier<T>> iterator;
		@Nullable
		private StreamSupplier<T> supplier;

		Concat(ChannelSupplier<StreamSupplier<T>> iterator) {
			this.iterator = iterator;
			next();
		}

		private void next() {
			this.supplier = null;
			iterator.get()
					.whenResult(supplier -> {
						if (supplier != null) {
							this.getEndOfStream()
									.whenException(supplier::closeEx);
							supplier.getEndOfStream()
									.whenResult(this::next)
									.whenException(this::closeEx);
							if (supplier.getEndOfStream().isComplete()) return;

							this.supplier = supplier;
							this.supplier.resume(getDataAcceptor());
						} else {
							sendEndOfStream();
						}
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onResumed() {
			if (supplier != null) {
				supplier.resume(getDataAcceptor());
			}
		}

		@Override
		protected void onSuspended() {
			if (supplier != null) {
				supplier.suspend();
			}
		}

		@Override
		protected void onError(Throwable e) {
			if (supplier != null) {
				supplier.closeEx(e);
			}
			iterator.closeEx(e);
		}

		@Override
		protected void onComplete() {
			iterator = null;
			supplier = null;
		}
	}

	static final class OfChannelSupplier<T> extends AbstractStreamSupplier<T> {
		private final ChannelSupplier<T> supplier;

		public OfChannelSupplier(ChannelSupplier<T> supplier) {
			this.supplier = supplier;
		}

		@Override
		protected void onResumed() {
			asyncBegin();
			supplier.get()
					.whenComplete((item, e) -> {
						if (e == null) {
							if (item != null) {
								send(item);
								asyncResume();
							} else {
								sendEndOfStream();
							}
						} else {
							closeEx(e);
						}
					});
		}

		@Override
		protected void onError(Throwable e) {
			supplier.closeEx(e);
		}
	}

}
