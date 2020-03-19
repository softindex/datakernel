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
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

final class StreamSuppliers {

	static final class ClosingWithError<T> extends AbstractStreamSupplier<T> {
		ClosingWithError(Throwable e) {
			super();
			closeEx(e);
		}
	}

	static final class Closing<T> extends AbstractStreamSupplier<T> {
		public Closing() {
			super();
			sendEndOfStream();
		}
	}

	static final class Idle<T> extends AbstractStreamSupplier<T> {
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
		private Promise<? extends StreamSupplier<T>> promise;
		private final InternalConsumer internalConsumer = new InternalConsumer();
		private class InternalConsumer extends AbstractStreamConsumer<T> {}

		public OfPromise(Promise<? extends StreamSupplier<T>> promise) {
			this.promise = promise;
		}

		@Override
		protected void onStarted() {
			promise
					.whenResult(supplier -> {
						this.getEndOfStream()
								.whenException(supplier::closeEx);
						supplier.getEndOfStream()
								.whenResult(this::sendEndOfStream)
								.whenException(this::closeEx);
						supplier.streamTo(internalConsumer);
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onResumed() {
			internalConsumer.resume(getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			internalConsumer.suspend();
		}

		@Override
		protected void onAcknowledge() {
			internalConsumer.acknowledge();
		}

		@Override
		protected void onCleanup() {
			promise = null;
		}
	}

	static final class Concat<T> extends AbstractStreamSupplier<T> {
		private ChannelSupplier<StreamSupplier<T>> iterator;
		private InternalConsumer internalConsumer = new InternalConsumer();
		private class InternalConsumer extends AbstractStreamConsumer<T> {}

		Concat(ChannelSupplier<StreamSupplier<T>> iterator) {
			this.iterator = iterator;
		}

		@Override
		protected void onStarted() {
			next();
		}

		private void next() {
			internalConsumer.acknowledge();
			internalConsumer = new InternalConsumer();
			resume();
			iterator.get()
					.whenResult(supplier -> {
						if (supplier != null) {
							supplier.getEndOfStream()
									.whenResult(this::next)
									.whenException(this::closeEx);
							supplier.streamTo(internalConsumer);
						} else {
							sendEndOfStream();
						}
					})
					.whenException(this::closeEx);
		}

		@Override
		protected void onResumed() {
			internalConsumer.resume(getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			internalConsumer.suspend();
		}

		@Override
		protected void onError(Throwable e) {
			internalConsumer.closeEx(e);
			iterator.closeEx(e);
		}

		@Override
		protected void onCleanup() {
			iterator = null;
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
