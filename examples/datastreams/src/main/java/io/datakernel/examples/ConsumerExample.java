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
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of creating the custom StreamConsumer. This implementation just outputs received data to the console.
 */
public class ConsumerExample<T> extends AbstractStreamConsumer<T> {
	static {
		LoggerConfigurer.enableLogging();
	}

	@Override
	protected void onStarted() {
		getSupplier().resume(x -> System.out.println("received: " + x));
	}

	@Override
	protected Promise<Void> onEndOfStream() {
		System.out.println("End of stream received");
		return Promise.complete();
	}

	@Override
	protected void onError(Throwable t) {
		System.out.println("Error handling logic must be here. No confirmation to upstream is needed");
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		StreamConsumer<Integer> consumer = new ConsumerExample<>();
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3);

		supplier.streamTo(consumer);

		eventloop.run();
	}
}
