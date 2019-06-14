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

package io.datakernel.examples;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamTransformer;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of creating custom StreamTransformer, which takes strings from input stream
 * and transforms strings to their length if particular length is less than MAX_LENGTH
 */
public final class TransformerExample implements StreamTransformer<String, Integer> {
	private static final int MAX_LENGTH = 10;
	static {
		LoggerConfigurer.enableSLF4Jbridge();
	}

	private final AbstractStreamConsumer<String> inputConsumer = new AbstractStreamConsumer<String>() {

		@Override
		protected Promise<Void> onEndOfStream() {
			outputSupplier.sendEndOfStream();
			return Promise.complete();
		}

		@Override
		protected void onError(Throwable t) {
			System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
		}
	};

	private final AbstractStreamSupplier<Integer> outputSupplier = new AbstractStreamSupplier<Integer>() {

		@Override
		protected void onSuspended() {
			inputConsumer.getSupplier().suspend();
		}

		@Override
		protected void produce(AsyncProduceController async) {
			inputConsumer.getSupplier()
					.resume(item -> {
						int len = item.length();
						if (len < MAX_LENGTH) {
							send(len);
						}
					});
		}

		@Override
		protected void onError(Throwable t) {
			System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
		}
	};

	@Override
	public StreamConsumer<String> getInput() {
		return inputConsumer;
	}

	@Override
	public StreamSupplier<Integer> getOutput() {
		return outputSupplier;
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamSupplier<String> source = StreamSupplier.of("testdata", "testdata1", "testdata1000");

		TransformerExample transformer = new TransformerExample();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		source.transformWith(transformer).streamTo(consumer);

		consumer.getResult().whenResult(System.out::println);

		eventloop.run();
	}
}

