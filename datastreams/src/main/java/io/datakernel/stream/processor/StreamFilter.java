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

package io.datakernel.stream.processor;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

/**
 * Provides you to filter data for sending. It checks predicate's verity for inputting data and if
 * predicate is true sends data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams result set to the destination .
 *
 * @param <T>
 */
public final class StreamFilter<T> implements StreamTransformer<T, T> {
	private final Eventloop eventloop;
	private final Input input;
	private final Output output;
	private final Predicate<T> predicate;

	// region creators
	private StreamFilter(Eventloop eventloop, Predicate<T> predicate) {
		this.eventloop = eventloop;
		this.predicate = predicate;
		this.input = new Input(eventloop);
		this.output = new Output(eventloop);
	}

	@Override
	public StreamConsumer<T> getInput() {
		return input;
	}

	@Override
	public StreamProducer<T> getOutput() {
		return output;
	}

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop eventloop in which filter will be running
	 * @param predicate predicate for filtering data
	 */
	public static <T> StreamFilter<T> create(Eventloop eventloop, Predicate<T> predicate) {
		return new StreamFilter<>(eventloop, predicate);
	}
	// endregion

	protected final class Input extends AbstractStreamConsumer<T> {
		protected Input(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onEndOfStream() {
			output.sendEndOfStream();
		}

		@Override
		protected void onError(Exception e) {
			output.closeWithError(e);
		}

	}

	protected final class Output extends AbstractStreamProducer<T> {
		protected Output(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onSuspended() {
			input.getProducer().suspend();
		}

		@Override
		protected void onError(Exception e) {
			input.closeWithError(e);
		}

		@Override
		protected void onProduce(StreamDataReceiver<T> dataReceiver) {
			if (predicate == Predicates.alwaysTrue()) {
				input.getProducer().produce(dataReceiver);
			} else {
				Predicate<T> predicate = StreamFilter.this.predicate;
				input.getProducer().produce(item -> {
					if (predicate.apply(item)) {
						dataReceiver.onData(item);
					}
				});
			}
		}
	}
}
