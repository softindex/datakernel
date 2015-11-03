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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.TestStreamConsumers;

import static io.datakernel.stream.StreamProducers.ofIterable;
import static java.util.Arrays.asList;

/**
 * Example 4.
 * Example of creating custom StreamTransformer, which takes strings from input stream
 * and transforms strings to their length if particular length is less than MAX_LENGH
 */
public final class TransformerExample extends AbstractStreamTransformer_1_1<String, Integer> {

	private static final int MAX_LENGTH = 10;

	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	protected final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.sendEndOfStream();
		}

		@SuppressWarnings("unchecked")
		@Override
		public StreamDataReceiver<String> getDataReceiver() {
			return outputProducer;
		}
	}

	protected final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<String> {

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}

		@Override
		public void onData(String item) {
			int len = item.length();
			if (len < MAX_LENGTH) {
				send(len);
			}
		}
	}

	protected TransformerExample(Eventloop eventloop) {
		super(eventloop);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer();
	}

	public static void main(String[] args) {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<String> source = ofIterable(eventloop, asList("testdata", "testdata1", "testdata1000"));

		TransformerExample transformer = new TransformerExample(eventloop);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(transformer.getInput());
		transformer.getOutput().streamTo(consumer);

		eventloop.run();

		System.out.println(consumer.getList());
	}
}

