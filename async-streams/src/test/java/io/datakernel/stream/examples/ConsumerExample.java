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

package io.datakernel.stream.examples;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;

/**
 * Example 2.
 * Example of creating the custom StreamConsumer. This implementation just outputs received data to the console.
 */
public class ConsumerExample<T> extends AbstractStreamConsumer<T> implements StreamDataReceiver<T> {

	public ConsumerExample(Eventloop eventloop) {
		super(eventloop);
	}

	@Override
	protected void onStarted() {

	}

	@Override
	public StreamDataReceiver<T> getDataReceiver() {
		return this;
	}

	@Override
	public void onData(T item) {
		System.out.println(item.toString());
	}

//	@Override
//	public void onProducerEndOfStream() {
//		System.out.println("End of stream received. " +
//				"StreamConsumer must be acked and closed by replying 'finish' to upstream");
//		close();
//	}

	@Override
	protected void onEndOfStream() {
		System.out.println("End of stream received. " +
				"StreamConsumer must be acked and closed by replying 'finish' to upstream");
		close();
	}

//	@Override
//	public void onProducerError(Exception e) {
//		System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
//		closeWithError(e);
//	}

	@Override
	protected void onError(Exception e) {
		System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
	}
}
