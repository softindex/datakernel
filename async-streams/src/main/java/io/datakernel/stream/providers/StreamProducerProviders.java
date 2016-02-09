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

package io.datakernel.stream.providers;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class StreamProducerProviders {

	private StreamProducerProviders() {

	}

	public static StreamProducerProvider<ByteBuf> ofFile(Eventloop eventloop, ExecutorService executor, Path filePath) {
		return new OfFile(eventloop, executor, filePath);
	}

	public static <T> StreamProducerProvider<T> ofIterable(Eventloop eventloop, Iterable<T> iterable) {
		return new StreamProducerOfIterable<>(eventloop, iterable);
	}

	public static class OfFile implements StreamProducerProvider<ByteBuf> {

		private final Eventloop eventloop;
		private final ExecutorService executor;
		private final Path filePath;

		public OfFile(Eventloop eventloop, ExecutorService executor, Path filePath) {
			this.eventloop = eventloop;
			this.executor = executor;
			this.filePath = filePath;
		}

		@Override
		public StreamProducer<ByteBuf> getProducer() {
			try {
				return StreamFileReader.readFileFully(eventloop, executor, 256 * 1024, filePath);
			} catch (IOException e) {
				return StreamProducers.closingWithError(eventloop, e);
			}
		}
	}

	public static class StreamProducerOfIterable<T> implements StreamProducerProvider<T> {

		private final Eventloop eventloop;
		private final Iterable<T> iterable;

		public StreamProducerOfIterable(Eventloop eventloop, Iterable<T> iterable) {
			this.eventloop = eventloop;
			this.iterable = iterable;
		}

		@Override
		public StreamProducer<T> getProducer() {
			return StreamProducers.ofIterable(eventloop, iterable);
		}
	}

}
