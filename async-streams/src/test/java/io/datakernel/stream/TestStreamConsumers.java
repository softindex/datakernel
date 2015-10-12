/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;

public class TestStreamConsumers {
	public static abstract class TestConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		protected final List<T> list;

		abstract public void onData(T item);

		public TestConsumerToList(Eventloop eventloop, List<T> list) {
			super(eventloop);
			this.list = list;
		}

		public TestConsumerToList(Eventloop eventloop) {
			this(eventloop, new ArrayList<T>());
		}

		@Override
		protected final void onStarted() {

		}

		@Override
		protected final void onEndOfStream() {
		}

		@Override
		protected final void onError(Exception e) {
		}

		@Override
		public final StreamDataReceiver<T> getDataReceiver() {
			return this;
		}

		public final List<T> getList() {
			checkState(getConsumerStatus() == StreamStatus.END_OF_STREAM, "ToList consumer is not closed");
			return list;
		}
	}

	public static <T> TestConsumerToList<T> toListOneByOne(final Eventloop eventloop, List<T> list) {
		return new TestConsumerToList<T>(eventloop, list) {
			@Override
			public void onData(T item) {
				list.add(item);
				this.suspend();
				this.eventloop.post(new Runnable() {
					@Override
					public void run() {
						resume();
					}
				});
			}
		};
	}

	public static <T> TestConsumerToList<T> toListOneByOne(Eventloop eventloop) {
		return toListOneByOne(eventloop, new ArrayList<T>());
	}

	public static <T> TestConsumerToList<T> toListRandomlySuspending(Eventloop eventloop, List<T> list, final Random random) {
		return new TestConsumerToList<T>(eventloop, list) {
			@Override
			public void onData(T item) {
				list.add(item);
				if (random.nextBoolean()) {
					suspend();
					this.eventloop.post(new Runnable() {
						@Override
						public void run() {
							resume();
						}
					});
				}
			}
		};
	}

	public static <T> TestConsumerToList<T> toListRandomlySuspending(Eventloop eventloop, List<T> list) {
		return toListRandomlySuspending(eventloop, list, new Random());
	}

	public static <T> TestConsumerToList<T> toListRandomlySuspending(Eventloop eventloop) {
		return toListRandomlySuspending(eventloop, new ArrayList<T>(), new Random());
	}

	public static <T> TestConsumerToList<T> toListErrorAtSecondItem(Eventloop eventloop) {
		return toListErrorAtSecondItem(eventloop, new ArrayList<T>());
	}

	public static <T> TestConsumerToList<T> toListErrorAtSecondItem(Eventloop eventloop, List<T> list) {
		return new TestConsumerToList<T>(eventloop, list) {
			@Override
			public void onData(T item) {
				list.add(item);
				if (list.size() == 2) {
					closeWithError(new Exception());
				}
			}
		};
	}
}
