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

package io.datakernel.stream.stats;

import io.datakernel.async.Promise;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamTransformer;

import java.util.Set;

import static java.util.Collections.emptySet;

public class StreamStatsForwarder<T> implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;

	private final StreamStats<T> stats;

	private StreamStatsForwarder(StreamStats<T> stats) {
		this.stats = stats;
		this.input = new Input();
		this.output = new Output();
	}

	public static <T> StreamStatsForwarder<T> create(StreamStats<T> stats) {
		return new StreamStatsForwarder<>(stats);
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamSupplier<T> getOutput() {
		return output;
	}

	private class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			stats.onStarted();
		}

		@Override
		protected Promise<Void> onEndOfStream() {
			stats.onEndOfStream();
			return output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable e) {
			output.close(e);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			StreamConsumer<T> consumer = output.getConsumer();
			return consumer != null ? consumer.getCapabilities() : emptySet();
		}
	}

	private class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
			stats.onProduce();
			input.getSupplier().resume(stats.createDataAcceptor(dataAcceptor));
		}

		@Override
		protected void onSuspended() {
			stats.onSuspend();
			input.getSupplier().suspend();
		}

		@Override
		protected void onError(Throwable e) {
			stats.onError(e);
			input.close(e);
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			StreamSupplier<T> supplier = input.getSupplier();
			return supplier != null ? supplier.getCapabilities() : emptySet();
		}
	}
}
