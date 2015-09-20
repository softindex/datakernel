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
import io.datakernel.stream.AbstractStreamTransformer_1_1_Stateless;
import io.datakernel.stream.StreamDataReceiver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides you apply function before sending data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams set of function's result  to the destination .
 *
 * @param <I> type of input data
 * @param <O> type of output data
 */
public final class StreamFunction<I, O> extends AbstractStreamTransformer_1_1_Stateless<I, O> implements StreamDataReceiver<I> {
	private final Function<I, O> function;

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop eventloop in which filter will be running
	 * @param function  function for applying
	 */
	public StreamFunction(Eventloop eventloop, Function<I, O> function) {
		super(eventloop);
		checkNotNull(function);
		this.function = function;
	}

	/**
	 * Returns callback for right sending data, if its function is identity, returns dataReceiver
	 * for sending data without filtering.
	 */
	@Override
	protected StreamDataReceiver<I> getUpstreamDataReceiver() {
		return function == Functions.identity() ? (StreamDataReceiver<I>) downstreamDataReceiver : this;
	}

	/**
	 * Applies function to received data and sends result to the destination
	 *
	 * @param item received data
	 */
	@Override
	public void onData(I item) {
		downstreamDataReceiver.onData(function.apply(item));
	}
}
