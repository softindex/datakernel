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

/**
 * Provides you apply function before sending data to the destination. It is a {@link AbstractStreamTransformer_1_1}
 * which receives specified type and streams set of function's result  to the destination .
 *
 * @param <T> type of data
 */
public final class StreamConsumerSwitcher<T> extends AbstractStreamTransformer_1_1_Stateless<T, T> implements StreamDataReceiver<T> {

	/**
	 * Creates a new instance of this class
	 *
	 * @param eventloop eventloop in which filter will be running
	 */
	public StreamConsumerSwitcher(Eventloop eventloop) {
		super(eventloop);
	}

	/**
	 * Returns callback for right sending data, if its function is identity, returns dataReceiver
	 * for sending data without filtering.
	 */
	@Override
	protected StreamDataReceiver<T> getUpstreamDataReceiver() {
		return downstreamDataReceiver;
	}

	@Override
	protected void onUpstreamEndOfStream() {
		downstreamProducer.sendEndOfStream();
		upstreamConsumer.close();
	}

	/**
	 * Applies function to received data and sends result to the destination
	 *
	 * @param item received data
	 */
	@Override
	public void onData(T item) {
		downstreamDataReceiver.onData(item);
	}
}
