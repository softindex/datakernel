/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.Recyclable;
import io.datakernel.common.exception.UncheckedException;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static io.datakernel.common.Recyclable.deepRecycle;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;

/**
 * Provides additional functionality for managing {@link ChannelConsumer}s.
 */
@SuppressWarnings("WeakerAccess")
public final class ChannelConsumers {

	/**
	 * Passes iterator's values to the {@code output} until it {@code hasNext()},
	 * then returns a promise of {@code null} as a marker of completion.
	 * <p>
	 * If there was an exception while accepting iterator, a promise of
	 * exception will be returned.
	 *
	 * @param output a {@code ChannelConsumer}, which accepts the iterator
	 * @param it     an {@link Iterator} which provides some values
	 * @param <T>    a data type of passed values
	 * @return a promise of {@code null} as a marker of completion
	 */
	public static <T> Promise<Void> acceptAll(ChannelConsumer<T> output, Iterator<? extends T> it) {
		if (!it.hasNext()) return Promise.complete();
		return Promise.ofCallback(cb -> acceptAllImpl(output, it, cb));
	}

	private static <T> void acceptAllImpl(ChannelConsumer<T> output, Iterator<? extends T> it, SettablePromise<Void> cb) {
		while (it.hasNext()) {
			Promise<Void> accept = output.accept(it.next());
			if (accept.isResult()) continue;
			accept.whenComplete(($, e) -> {
				if (e == null) {
					acceptAllImpl(output, it, cb);
				} else {
					deepRecycle(it);
					cb.setException(e);
				}
			});
			return;
		}
		cb.set(null);
	}

	public static <T extends Recyclable> ChannelConsumer<T> recycling() {
		return new RecyclingChannelConsumer<>();
	}

	public static ChannelConsumer<ByteBuf> outputStreamAsChannelConsumer(Executor executor, OutputStream outputStream) {
		return new AbstractChannelConsumer<ByteBuf>() {
			@Override
			protected Promise<Void> doAccept(@Nullable ByteBuf buf) {
				return Promise.ofBlockingRunnable(executor, () -> {
					try {
						if (buf != null) {
							outputStream.write(buf.array(), buf.head(), buf.readRemaining());
							buf.recycle();
						} else {
							outputStream.close();
						}
					} catch (IOException e) {
						throw new UncheckedException(e);
					}
				});
			}

			@Override
			protected void onClosed(@NotNull Throwable e) {
				executor.execute(() -> {
					try {
						outputStream.close();
					} catch (IOException ignored) {
					}
				});
			}
		};
	}

	public static OutputStream channelConsumerAsOutputStream(Eventloop eventloop, ChannelConsumer<ByteBuf> channelConsumer) {
		return new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				write(new byte[]{(byte) b}, 0, 1);
			}

			@Override
			public void write(@NotNull byte[] b, int off, int len) throws IOException {
				submit(ByteBuf.wrap(b, off, off + len));
			}

			@Override
			public void close() throws IOException {
				submit(null);
			}

			private void submit(ByteBuf buf) throws IOException {
				CompletableFuture<Void> future = eventloop.submit(() -> channelConsumer.accept(buf));
				try {
					future.get();
				} catch (InterruptedException e) {
					eventloop.execute(wrapContext(channelConsumer, channelConsumer::cancel));
					throw new IOException(e);
				} catch (ExecutionException e) {
					Throwable cause = e.getCause();
					if (cause instanceof IOException) throw (IOException) cause;
					if (cause instanceof RuntimeException) throw (RuntimeException) cause;
					if (cause instanceof Exception) throw new IOException(cause);
					throw (Error) cause;
				}
			}
		};
	}
}
