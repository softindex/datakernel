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

package io.datakernel.serial;

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;

import java.util.Iterator;

public abstract class ByteBufsSupplier implements Cancellable {
	public static final Exception UNEXPECTED_DATA_EXCEPTION = new ParseException(ByteBufsSupplier.class, "Unexpected data after end-of-stream");
	public static final Exception UNEXPECTED_END_OF_STREAM_EXCEPTION = new ParseException(ByteBufsSupplier.class, "Unexpected end-of-stream");

	protected final ByteBufQueue bufs;

	protected ByteBufsSupplier(ByteBufQueue bufs) {
		this.bufs = bufs;
	}

	protected ByteBufsSupplier() {
		this.bufs = new ByteBufQueue();
	}

	public ByteBufQueue getBufs() {
		return bufs;
	}

	public abstract Promise<Void> needMoreData();

	public abstract Promise<Void> endOfStream();

	public static ByteBufsSupplier ofIterable(Iterable<ByteBuf> iterable) {
		return of(SerialSupplier.ofIterator(iterable.iterator()));
	}

	public static ByteBufsSupplier ofIterator(Iterator<ByteBuf> iterator) {
		return of(SerialSupplier.ofIterator(iterator));
	}

	public static ByteBufsSupplier of(SerialSupplier<ByteBuf> input) {
		return new ByteBufsSupplier() {
			@Override
			public Promise<Void> needMoreData() {
				return input.get()
						.thenCompose(buf -> {
							if (buf != null) {
								bufs.add(buf);
								return Promise.complete();
							} else {
								return Promise.ofException(UNEXPECTED_END_OF_STREAM_EXCEPTION);
							}
						});
			}

			@Override
			public Promise<Void> endOfStream() {
				if (!bufs.isEmpty()) {
					bufs.recycle();
					return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
				}
				return input.get()
						.thenCompose(buf -> {
							if (buf != null) {
								buf.recycle();
								return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
							} else {
								return Promise.complete();
							}
						});
			}

			@Override
			public void close(Throwable e) {
				bufs.recycle();
				input.close(e);
			}
		};
	}

	public static ByteBufsSupplier ofProvidedQueue(ByteBufQueue queue,
			AsyncSupplier<Void> get, AsyncSupplier<Void> complete, Cancellable cancellable) {
		return new ByteBufsSupplier(queue) {
			@Override
			public Promise<Void> needMoreData() {
				return get.get();
			}

			@Override
			public Promise<Void> endOfStream() {
				return complete.get();
			}

			@Override
			public void close(Throwable e) {
				cancellable.close(e);
			}
		};
	}

	public final <T> Promise<T> parse(ByteBufsParser<T> parser) {
		if (!bufs.isEmpty()) {
			T result;
			try {
				result = parser.tryParse(bufs);
			} catch (Exception e) {
				return Promise.ofException(e);
			}
			if (result != null) {
				return Promise.of(result);
			}
		}
		SettablePromise<T> cb = new SettablePromise<>();
		doParse(parser, cb);
		return cb;
	}

	private <T> void doParse(ByteBufsParser<T> parser, SettablePromise<T> cb) {
		needMoreData()
				.whenComplete(($, e) -> {
					if (e == null) {
						T result;
						try {
							result = parser.tryParse(bufs);
						} catch (Exception e2) {
							close(e2);
							cb.setException(e2);
							return;
						}
						if (result == null) {
							doParse(parser, cb);
							return;
						}
						cb.set(result);
					} else {
						cb.setException(e);
					}
				});
	}

	public final <T> Promise<T> parseRemaining(ByteBufsParser<T> parser) {
		return parse(parser)
				.thenCompose(result -> {
					if (!bufs.isEmpty()) {
						close(UNEXPECTED_DATA_EXCEPTION);
						return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
					}
					return endOfStream().thenApply($ -> result);
				});
	}

	public final <T> SerialSupplier<T> parseStream(ByteBufsParser<T> parser) {
		return SerialSupplier.of(
				() -> parse(parser)
						.thenComposeEx((value, e) -> {
							if (e == null) return Promise.of(value);
							if (e == UNEXPECTED_END_OF_STREAM_EXCEPTION && bufs.isEmpty()) return Promise.of(null);
							return Promise.ofException(e);
						}),
				this);
	}

	public MaterializedPromise<Void> bindTo(ByteBufsInput input) {
		return input.set(this);
	}
}
