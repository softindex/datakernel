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

package io.datakernel.stream;

import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumerToCollector<T, A, R> extends AbstractStreamConsumer<T> {
	private final Collector<T, A, R> collector;
	private final SettablePromise<R> resultPromise = new SettablePromise<>();
	private A accumulator;

	public StreamConsumerToCollector(Collector<T, A, R> collector) {
		this.collector = collector;
	}

	@Override
	protected void onStarted() {
		accumulator = collector.supplier().get();
		BiConsumer<A, T> consumer = collector.accumulator();
		getSupplier().resume(item -> consumer.accept(accumulator, item));
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		R result = collector.finisher().apply(accumulator);
		//noinspection AssignmentToNull - resource release
		accumulator = null;
		resultPromise.set(result);
		return Promise.complete();
	}

	@Override
	protected void onError(Throwable e) {
		resultPromise.setException(e);
	}

	public MaterializedPromise<R> getResult() {
		return resultPromise;
	}

	public A getAccumulator() {
		return accumulator;
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return EnumSet.of(LATE_BINDING);
	}
}
