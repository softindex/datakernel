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

import io.datakernel.async.AsyncCallable;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AsyncStreamsTest {
	@Test
	public void testDelayedProducer() throws Exception {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> producer = StreamProducers.asynchronouslyResolving(eventloop, new AsyncCallable<StreamProducer<Integer>>() {
			@Override
			public void call(ResultCallback<StreamProducer<Integer>> callback) {
				callback.postResult(StreamProducers.ofIterable(eventloop, asList(1, 2, 3)));
			}
		});
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		producer.streamTo(consumer);

		eventloop.run();
		assertEquals(asList(1, 2, 3), consumer.getList());
		assertEquals(END_OF_STREAM, consumer.getUpstream().getProducerStatus());
	}

}