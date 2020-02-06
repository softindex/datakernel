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

import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.queue.ChannelQueue;
import io.datakernel.csp.queue.ChannelZeroBuffer;
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
		public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void close(@NotNull Throwable e) {
		}
	}

	static final class Closing<T> implements StreamSupplier<T> {
		@Override
		public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return Promise.complete();
		}

		@Override
		public void close(@NotNull Throwable e) {
		}
	}

	static final class Idle<T> implements StreamSupplier<T> {
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();

		@Override
		public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void close(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	static final class OfIterator<T> implements StreamSupplier<T> {
		private final Iterator<T> iterator;
		private @Nullable StreamDataAcceptor<T> dataAcceptor;
		private final SettablePromise<Void> endOfStream = new SettablePromise<>();
		private boolean iterating;

		{
			endOfStream
					.whenComplete(() -> dataAcceptor = null);
		}

		/**
		 * Creates a new instance of  StreamSupplierOfIterator
		 *
		 * @param iterator iterator with object which need to send
		 */
		public OfIterator(@NotNull Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
			if (endOfStream.isComplete()) return;
			if (this.dataAcceptor == dataAcceptor) return;
			this.dataAcceptor = dataAcceptor;
			if (iterating) return;
			iterating = true;
			while (iterator.hasNext()) {
				StreamDataAcceptor<T> acceptor = this.dataAcceptor;
				if (acceptor == null) {
					iterating = false;
					return;
				}
				T item = iterator.next();
				acceptor.accept(item);
			}
			endOfStream.trySet(null);
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void close(@NotNull Throwable e) {
			endOfStream.trySetException(e);
		}
	}

	static final class OfPromise<T> implements StreamSupplier<T> {
		@Nullable StreamSupplier<T> streamSupplier;
		@Nullable StreamDataAcceptor<T> dataAcceptor;
		SettablePromise<Void> endOfStream;

		public OfPromise(Promise<? extends StreamSupplier<T>> promise) {
			endOfStream = new SettablePromise<>();
			promise
					.whenResult(stream -> {
						stream.getEndOfStream().whenComplete(this.endOfStream::trySet);
						streamSupplier = stream;
						if (streamSupplier.getEndOfStream().isComplete()) {
							dataAcceptor = null;
							return;
						}
						if (endOfStream.getException() != null) {
							streamSupplier.close(endOfStream.getException());
						} else if (dataAcceptor != null) {
							StreamDataAcceptor<T> dataAcceptor = this.dataAcceptor;
							this.dataAcceptor = null;
							streamSupplier.supply(dataAcceptor);
						}
					})
					.whenException(this::close);
		}

		@Override
		public void supply(@Nullable StreamDataAcceptor<T> dataAcceptor) {
			if (streamSupplier != null) {
				streamSupplier.supply(dataAcceptor);
			} else {
				this.dataAcceptor = dataAcceptor;
			}
		}

		@Override
		public Promise<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public void close(@NotNull Throwable e) {
			endOfStream.trySetException(e);
			if (streamSupplier != null) {
				streamSupplier.close(e);
			}
		}
	}

	static final class OfChannelSupplier<T> extends AbstractStreamSupplier<T> {
		private final ChannelSupplier<T> supplier;

		public OfChannelSupplier(ChannelSupplier<T> supplier) {
			this.supplier = supplier;
		}

		@Override
		protected void onResumed(AsyncProduceController async) {
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
	}

	static final class AsChannelSupplier<T> extends AbstractChannelSupplier<T> {
		private final ChannelQueue<T> queue;
		private final StreamSupplier<T> streamSupplier;
		private boolean endOfStream;

		public AsChannelSupplier(StreamSupplier<T> supplier) {
			this(new ChannelZeroBuffer<>(), supplier);
		}

		public AsChannelSupplier(ChannelQueue<T> queue, StreamSupplier<T> supplier) {
			this.queue = queue;
			this.streamSupplier = supplier;
			this.streamSupplier.getEndOfStream()
					.whenResult(() -> endOfStream = true) // *
					.whenException(this::close); // *
			if (!this.streamSupplier.getEndOfStream().isComplete()) {
				this.streamSupplier.supply(item -> { // *
					assert !isClosed();
					assert !endOfStream;
					Promise<Void> promise = this.queue.put(item);
					if (!promise.isComplete()) {
						this.streamSupplier.supply(null);
					} else { // *
					}
				});
			}
		}

		@Override
		protected @NotNull Promise<T> doGet() {
			return !endOfStream ? queue.take() : Promise.of(null);
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
			streamSupplier.close(e); // *
		}
	}

}
