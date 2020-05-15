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

import io.datakernel.async.function.AsyncSupplier;
import io.datakernel.async.process.AsyncCloseable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public abstract class BinaryChannelSupplier implements AsyncCloseable {
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
					input.closeEx(UNEXPECTED_DATA_EXCEPTION);
					return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
				}
				return input.get()
						.then(buf -> {
							if (buf == null) {
								return Promise.complete();
							} else {
								buf.recycle();
								input.closeEx(UNEXPECTED_DATA_EXCEPTION);
								return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
							}
						});
			}

			@Override
			public void closeEx(@NotNull Throwable e) {
				bufs.recycle();
				input.closeEx(e);
			}
		};
	}

	public static BinaryChannelSupplier ofProvidedQueue(ByteBufQueue queue,
			AsyncSupplier<Void> get, AsyncSupplier<Void> complete, AsyncCloseable closeable) {
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
			public void closeEx(@NotNull Throwable e) {
				closeable.closeEx(e);
			}
		};
	}

	public final <T> Promise<T> parse(ByteBufsDecoder<T> decoder) {
		while (true) {
			if (!bufs.isEmpty()) {
				T result;
				try {
					result = decoder.tryDecode(bufs);
				} catch (Exception e) {
					closeEx(e);
					return Promise.ofException(e);
				}
				if (result != null) {
					return Promise.of(result);
				}
			}
			Promise<Void> moreDataPromise = needMoreData();
			if (moreDataPromise.isResult()) continue;
			return moreDataPromise
					.whenException(this::close)
					.then(() -> parse(decoder));
		}
	}

	public final <T> Promise<T> parseRemaining(ByteBufsDecoder<T> decoder) {
		return parse(decoder)
				.then(result -> {
					if (!bufs.isEmpty()) {
						closeEx(UNEXPECTED_DATA_EXCEPTION);
						return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
					}
					return endOfStream().map($ -> result);
				});
	}

	public final <T> ChannelSupplier<T> parseStream(ByteBufsDecoder<T> decoder) {
		return ChannelSupplier.of(
				() -> parse(decoder)
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
