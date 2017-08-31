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

package io.datakernel.aggregation;

import io.datakernel.aggregation.ot.AggregationStructure;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.IRemoteFsClient;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static io.datakernel.aggregation.AggregationUtils.createBufferSerializer;

public class RemoteFsChunkStorage implements AggregationChunkStorage {
	private final Eventloop eventloop;
	private final IRemoteFsClient client;
	private final IdGenerator<Long> idGenerator;

	private RemoteFsChunkStorage(Eventloop eventloop, IdGenerator<Long> idGenerator, InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.idGenerator = idGenerator;
		this.client = RemoteFsClient.create(eventloop, serverAddress);
	}

	public static RemoteFsChunkStorage create(Eventloop eventloop, IdGenerator<Long> idGenerator, InetSocketAddress serverAddress) {
		return new RemoteFsChunkStorage(eventloop, idGenerator, serverAddress);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> void read(final AggregationStructure aggregation, final List<String> fields,
	                     final Class<T> recordClass, long id, final DefiningClassLoader classLoader,
	                     final ResultCallback<StreamProducer<T>> callback) {
		client.download(path(id), 0).whenComplete((producer, throwable) -> {
			if (throwable == null) {
				StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);
				producer.streamTo(decompressor.getInput());

				BufferSerializer<T> bufferSerializer = createBufferSerializer(aggregation, recordClass,
						aggregation.getKeys(), fields, classLoader);
				StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);

				decompressor.getOutput().streamTo(deserializer.getInput());
				callback.setResult(deserializer.getOutput());
			} else {
				callback.setException(AsyncCallbacks.throwableToException(throwable));
			}
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> void write(StreamProducer<T> producer, AggregationStructure aggregation, List<String> fields, Class<T> recordClass,
	                      final long id, DefiningClassLoader classLoader, final CompletionCallback callback) {
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		BufferSerializer<T> bufferSerializer = createBufferSerializer(aggregation, recordClass,
				aggregation.getKeys(), fields, classLoader);
		StreamBinarySerializer<T> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer)
				.withDefaultBufferSize(StreamBinarySerializer.MAX_SIZE_2)
				.withFlushDelay(1000);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(compressor.getInput());

		client.upload(compressor.getOutput(), path(id)).whenComplete((aVoid, throwable) -> {
			if (throwable == null) {
				callback.setComplete();
			} else {
				client.delete(path(id));
				callback.setException(AsyncCallbacks.throwableToException(throwable));
			}
		});
	}

	private String path(long id) {
		return id + ".log";
	}

	@Override
	public CompletionStage<Long> createId() {
		return idGenerator.createId();
	}
}