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

import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.util.Recyclable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

import static io.datakernel.util.Recyclable.deepRecycle;

public final class ChannelConsumers {
	private ChannelConsumers() {
	}

	public static <T> Promise<Void> acceptAll(ChannelConsumer<T> output, Iterator<? extends T> it) {
		if (!it.hasNext()) return Promise.complete();
		SettablePromise<Void> result = new SettablePromise<>();
		acceptAllImpl(output, it, result);
		return result;
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

	static final class Recycler<T extends Recyclable> implements ChannelConsumer<T> {
		@Override
		public @NotNull Promise<Void> accept(@Nullable Recyclable value) {
			if (value != null) {
				value.recycle();
			}
			return Promise.complete();
		}

		@Override
		public void close(@NotNull Throwable e) {
		}
	}

	static final class Lenient<T extends Recyclable> implements ChannelConsumer<T> {
		final ChannelConsumer<T> peer;
		boolean stop = false;

		Lenient(ChannelConsumer<T> peer) {
			this.peer = peer;
		}

		@Override
		public @NotNull Promise<Void> accept(@Nullable T value) {
			if (stop) {
				if (value != null) {
					value.recycle();
				}
				return Promise.complete();
			}
			return peer.accept(value)
					.thenComposeEx(($, e) -> {
						if (e != null) {
							stop = true;
						}
						return Promise.complete();
					});
		}

		@Override
		public void close(@NotNull Throwable e) {
			peer.close(e);
		}
	}


	/** A hacky optimization */
	public static boolean isRecycler(ChannelConsumer<? extends Recyclable> consumer) {
		return consumer instanceof Recycler || (consumer instanceof Lenient && ((Lenient) consumer).stop);
	}

	public static <T extends Recyclable> ChannelConsumer<T> recycling() {
		return new Recycler<>();
	}

	public static <T extends Recyclable> ChannelConsumer<T> lenient(ChannelConsumer<T> consumer) {
		return new Lenient<>(consumer);
	}
}
