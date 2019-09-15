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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import static io.datakernel.datastream.StreamCapability.LATE_BINDING;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

public final class StreamSuppliers {

	/**
	 * Represent supplier which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	static class ClosingWithErrorImpl<T> implements StreamSupplier<T> {
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();

		private final Throwable exception;

		ClosingWithErrorImpl(Throwable e) {
			this.exception = e;
		}

		@Override
		public void setConsumer(@NotNull StreamConsumer<T> consumer) {
			getCurrentEventloop().post(() -> endOfStream.trySetException(exception));
		}

		@Override
		public void resume(StreamDataAcceptor<T> dataAcceptor) {
			// do nothing
		}

		@Override
		public void suspend() {
			// do nothing
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void close(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	/**
	 * Represent supplier which sends specified exception to consumer.
	 *
	 * @param <T>
	 */
	static class ClosingImpl<T> implements StreamSupplier<T> {
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void setConsumer(@NotNull StreamConsumer<T> consumer) {
			getCurrentEventloop().post(() -> endOfStream.trySet(null));
		}

		@Override
		public void resume(StreamDataAcceptor<T> dataAcceptor) {
			// do nothing
		}

		@Override
		public void suspend() {
			// do nothing
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void close(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	static final class IdleImpl<T> implements StreamSupplier<T> {
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void setConsumer(@NotNull StreamConsumer<T> consumer) {
			consumer.getAcknowledgement()
					.whenException(endOfStream::trySetException);
		}

		@Override
		public void resume(StreamDataAcceptor<T> dataAcceptor) {
		}

		@Override
		public void suspend() {
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public void close(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	/**
	 * Represents a {@link AbstractStreamSupplier} which will send all values from iterator.
	 *
	 * @param <T> type of output data
	 */
	static class OfIteratorImpl<T> extends AbstractStreamSupplier<T> {
		private final Iterator<T> iterator;

		/**
		 * Creates a new instance of  StreamSupplierOfIterator
		 *
		 * @param iterator iterator with object which need to send
		 */
		public OfIteratorImpl(@NotNull Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		protected void produce(AsyncProduceController async) {
			while (iterator.hasNext()) {
				StreamDataAcceptor<T> dataAcceptor = getCurrentDataAcceptor();
				if (dataAcceptor == null) {
					return;
				}
				T item = iterator.next();
				dataAcceptor.accept(item);
			}
			sendEndOfStream();
		}

		@Override
		protected void onError(Throwable e) {
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	static class OfChannelSupplierImpl<T> extends AbstractStreamSupplier<T> {
		private final ChannelSupplier<T> supplier;

		public OfChannelSupplierImpl(ChannelSupplier<T> supplier) {
			this.supplier = supplier;
		}

		@Override
		protected void produce(AsyncProduceController async) {
			async.begin();
			supplier.get()
					.whenComplete((item, e) -> {
						if (e == null) {
							if (item != null) {
								send(item);
								async.resume();
							} else {
								sendEndOfStream();
							}
						} else {
							close(e);
						}
					});
		}

		@Override
		protected void onError(Throwable e) {
			supplier.close(e);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

}
