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

package io.datakernel.datastream.stats;

import io.datakernel.datastream.AbstractStreamConsumer;
import io.datakernel.datastream.AbstractStreamSupplier;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.datastream.processor.StreamTransformer;

public class StreamStatsForwarder<T> implements StreamTransformer<T, T> {
	private final Input input;
	private final Output output;

	private final StreamStats<T> stats;

	private StreamStatsForwarder(StreamStats<T> stats) {
		this.stats = stats;
		this.input = new Input();
		this.output = new Output();
		input.getAcknowledgement()
				.whenException(output::closeEx);
		output.getEndOfStream()
				.whenResult(input::acknowledge)
				.whenException(input::closeEx);
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

	protected final class Input extends AbstractStreamConsumer<T> {
		@Override
		protected void onStarted() {
			stats.onStarted();
			resume(output.getDataAcceptor());
		}

		@Override
		protected void onEndOfStream() {
			stats.onEndOfStream();
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Throwable e) {
			stats.onError(e);
		}
	}

	protected final class Output extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			stats.onProduce();
			input.resume(getDataAcceptor());
		}

		@Override
		protected void onSuspended() {
			stats.onSuspend();
			input.suspend();
		}

		@Override
		protected void onError(Throwable e) {
			stats.onError(e);
		}
	}

}
