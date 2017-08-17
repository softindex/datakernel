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

package io.datakernel.logfs.ot;

import io.datakernel.aggregation.util.AsyncResultsReducer;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_N;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

@SuppressWarnings("unchecked")
public abstract class LogDataConsumerSplitter<T, D> implements LogDataConsumer<T, D> {
	protected final Eventloop eventloop;

	protected LogDataConsumerSplitter(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@Override
	public final CompletionStage<List<D>> consume(StreamProducer<T> logStream) {
		AsyncResultsReducer<List<D>> resultsReducer = AsyncResultsReducer.<List<D>>create(new ArrayList<>());
		AbstractSplitter splitter = createSplitter(resultsReducer);
		logStream.streamTo(splitter.getInput());
		return resultsReducer.getResult();
	}

	protected abstract AbstractSplitter createSplitter(AsyncResultsReducer<List<D>> resultsReducer);

	protected abstract class AbstractSplitter extends AbstractStreamTransformer_1_N<T> implements StreamDataReceiver<T> {
		private final AsyncResultsReducer<List<D>> resultsReducer;

		protected AbstractSplitter(Eventloop eventloop, AsyncResultsReducer<List<D>> resultsReducer) {
			super(eventloop);
			this.resultsReducer = resultsReducer;
			setInputConsumer(new Input());
		}

		protected final <X> StreamDataReceiver<X> addOutput(LogDataConsumer<X, D> logDataConsumer) {
			Output<X> output = new Output<>();
			addOutput(output);
			resultsReducer.addStage(logDataConsumer.consume(output), (accumulator, diffs) -> {
				accumulator.addAll(diffs);
				return accumulator;
			});
			return output.getDownstreamDataReceiver();
		}

		private final class Input extends AbstractInputConsumer {
			@Override
			public StreamDataReceiver<T> getDataReceiver() {
				return AbstractSplitter.this;
			}

			@Override
			protected void onUpstreamEndOfStream() {
				sendEndOfStreamToDownstreams();
			}
		}

		private final class Output<X> extends AbstractOutputProducer<X> {
			@Override
			protected void onDownstreamSuspended() {
				inputConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				if (allOutputsResumed()) {
					inputConsumer.resume();
				}
			}
		}
	}
}
