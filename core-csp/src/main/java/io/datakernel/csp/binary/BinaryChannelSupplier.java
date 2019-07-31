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

package io.datakernel.csp.binary;

import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ParseException;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public abstract class BinaryChannelSupplier implements Cancellable {
	public static final Exception UNEXPECTED_DATA_EXCEPTION = new ParseException(BinaryChannelSupplier.class, "Unexpected data after end-of-stream");
	public static final Exception UNEXPECTED_END_OF_STREAM_EXCEPTION = new ParseException(BinaryChannelSupplier.class, "Unexpected end-of-stream");

	protected final ByteBufQueue bufs;

	protected BinaryChannelSupplier(ByteBufQueue bufs) {
		this.bufs = bufs;
	}

	protected BinaryChannelSupplier() {
		this.bufs = new ByteBufQueue();
	}

	public ByteBufQueue getBufs() {
		return bufs;
	}

	public abstract Promise<Void> needMoreData();

	public abstract Promise<Void> endOfStream();

	public static BinaryChannelSupplier ofIterable(Iterable<ByteBuf> iterable) {
		return of(ChannelSupplier.ofIterator(iterable.iterator()));
	}

	public static BinaryChannelSupplier ofIterator(Iterator<ByteBuf> iterator) {
		return of(ChannelSupplier.ofIterator(iterator));
	}

	public static BinaryChannelSupplier of(ChannelSupplier<ByteBuf> input) {
		return new BinaryChannelSupplier() {
			@Override
			public Promise<Void> needMoreData() {
				return input.get()
						.then(buf -> {
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
					input.close(UNEXPECTED_DATA_EXCEPTION);
					return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
				}
				return input.get()
						.then(buf -> {
							if (buf == null) {
								return Promise.complete();
							} else {
								buf.recycle();
								input.close(UNEXPECTED_DATA_EXCEPTION);
								return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
							}
						});
			}

			@Override
			public void close(@NotNull Throwable e) {
				bufs.recycle();
				input.close(e);
			}
		};
	}

	public static BinaryChannelSupplier ofProvidedQueue(ByteBufQueue queue,
			AsyncSupplier<Void> get, AsyncSupplier<Void> complete, Cancellable cancellable) {
		return new BinaryChannelSupplier(queue) {
			@Override
			public Promise<Void> needMoreData() {
				return get.get();
			}

			@Override
			public Promise<Void> endOfStream() {
				return complete.get();
			}

			@Override
			public void close(@NotNull Throwable e) {
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
		return Promise.ofCallback(cb -> doParse(parser, cb));
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
				.then(result -> {
					if (!bufs.isEmpty()) {
						close(UNEXPECTED_DATA_EXCEPTION);
						return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
					}
					return endOfStream().map($ -> result);
				});
	}

	public final <T> ChannelSupplier<T> parseStream(ByteBufsParser<T> parser) {
		return ChannelSupplier.of(
				() -> parse(parser)
						.thenEx((value, e) -> {
							if (e == null) return Promise.of(value);
							if (e == UNEXPECTED_END_OF_STREAM_EXCEPTION && bufs.isEmpty()) return Promise.of(null);
							return Promise.ofException(e);
						}),
				this);
	}

	public Promise<Void> bindTo(BinaryChannelInput input) {
		return input.set(this);
	}
}
