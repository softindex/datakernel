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

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TestStreamConsumers {
	public static abstract class TestConsumerToList<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {
		protected final List<T> list;
		private SettableStage<List<T>> resultStage = SettableStage.create();

		public TestConsumerToList(Eventloop eventloop) {
			this(eventloop, new ArrayList<>());
		}

		public TestConsumerToList(Eventloop eventloop, List<T> list) {
			super(eventloop);
			checkNotNull(list);
			this.list = list;
		}

		public CompletionStage<List<T>> getResultStage() {
			return resultStage;
		}

		@Override
		protected void onStarted() {
			getProducer().produce(this);
		}

		@Override
		protected void onEndOfStream() {
			if (resultStage != null) {
				resultStage.set(list);
			}
		}

		@Override
		protected void onError(Exception e) {
			if (resultStage != null) {
				resultStage.setException(e);
			}
		}

		public final List<T> getList() {
			checkState(getStatus() == StreamStatus.END_OF_STREAM, "ToList consumer is not closed");
			return list;
		}

		@Override
		public void onData(T item) {
			list.add(item);
		}
	}

	public static <T> TestConsumerToList<T> toListOneByOne(final Eventloop eventloop, List<T> list) {
		return new TestConsumerToList<T>(eventloop, list) {
			@Override
			public void onData(T item) {
				list.add(item);
				getProducer().suspend();
				this.eventloop.post(() -> getProducer().produce(this));
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
					getProducer().suspend();
					this.eventloop.post(() -> getProducer().produce(this));
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
}
