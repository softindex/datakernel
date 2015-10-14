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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides you apply function before sending data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams set of function's result  to the destination .
 *
 * @param <I> type of input data
 * @param <O> type of output data
 */
public final class StreamFunction<I, O> extends AbstractStreamTransformer_1_1<I, O> {
	private final UpstreamConsumer upstreamConsumer;
	private final DownstreamProducer downstreamProducer;

	protected final class UpstreamConsumer extends AbstractUpstreamConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			downstreamProducer.sendEndOfStream();
		}

		@SuppressWarnings("unchecked")
		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return downstreamProducer.function == Functions.identity() ?
					(StreamDataReceiver<I>) downstreamProducer.getDownstreamDataReceiver() :
					downstreamProducer;
		}
	}

	protected final class DownstreamProducer extends AbstractDownstreamProducer implements StreamDataReceiver<I> {
		private final Function<I, O> function;

		public DownstreamProducer(Function<I, O> function) {this.function = checkNotNull(function);}

		@Override
		protected void onDownstreamSuspended() {
			upstreamConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			upstreamConsumer.resume();
		}

		@Override
		public void onData(I item) {
			send(function.apply(item));
		}
	}

	public StreamFunction(Eventloop eventloop, Function<I, O> function) {
		super(eventloop);
		this.upstreamConsumer = new UpstreamConsumer();
		this.downstreamProducer = new DownstreamProducer(function);
	}

}
