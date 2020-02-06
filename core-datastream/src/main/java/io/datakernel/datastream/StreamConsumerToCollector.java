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

import io.datakernel.promise.Promise;
import io.datakernel.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;
import java.util.stream.Collector;

final class StreamConsumerToCollector<T, A, R> implements StreamConsumer<T> {
	private final Collector<T, A, R> collector;
	private final SettablePromise<R> resultPromise = new SettablePromise<>();
	private final Promise<Void> acknowledgement = resultPromise.toVoid();
	@Nullable
	private A accumulator;

	public StreamConsumerToCollector(Collector<T, A, R> collector) {
		this.collector = collector;
	}

	@Override
	public void consume(@NotNull StreamDataSource<T> dataSource) {
		A accumulator = collector.supplier().get();
		this.accumulator = accumulator;
		BiConsumer<A, T> consumer = collector.accumulator();
		dataSource.resume(item -> consumer.accept(accumulator, item));
	}

	@Override
	public void endOfStream() {
		if (resultPromise.isComplete()) return;
		R result = collector.finisher().apply(accumulator);
		accumulator = null;
		resultPromise.set(result);
	}

	@Override
	public Promise<Void> getAcknowledgement() {
		return acknowledgement;
	}

	@Override
	public void close(@NotNull Throwable e) {
		resultPromise.trySetException(e);
	}

	public Promise<R> getResult() {
		return resultPromise;
	}

	@Nullable
	public A getAccumulator() {
		return accumulator;
	}

}
