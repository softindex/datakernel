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
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.IRemoteFsClient;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static io.datakernel.aggregation.AggregationUtils.createBufferSerializer;

public class RemoteFsChunkStorage implements AggregationChunkStorage {
	public static final String LOG = ".log";
	public static final String TEMP_LOG = ".temp";

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

	@Override
	public <T> CompletionStage<StreamProducerWithResult<T, Void>> read(AggregationStructure aggregation, List<String> fields,
	                                                                   Class<T> recordClass, long chunkId,
	                                                                   DefiningClassLoader classLoader) {
		return client.download(path(chunkId), 0).thenApply(producer -> {
			StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);

			BufferSerializer<T> bufferSerializer = createBufferSerializer(aggregation, recordClass,
					aggregation.getKeys(), fields, classLoader);
			StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);

			producer.streamTo(decompressor.getInput());
			decompressor.getOutput().streamTo(deserializer.getInput());

			return StreamProducers.withEndOfStream(deserializer.getOutput());
		});
	}

	private String path(long chunkId) {
		return chunkId + LOG;
	}

	@Override
	public <T> CompletionStage<StreamConsumerWithResult<T, Void>> write(AggregationStructure aggregation, List<String> fields,
	                                                                    Class<T> recordClass, long chunkId,
	                                                                    DefiningClassLoader classLoader) {
		return client.upload(tempPath(chunkId)).thenApply(consumer -> {
			StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
			BufferSerializer<T> bufferSerializer = createBufferSerializer(aggregation, recordClass,
					aggregation.getKeys(), fields, classLoader);

			StreamBinarySerializer<T> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer)
					.withDefaultBufferSize(StreamBinarySerializer.MAX_SIZE_2);

			serializer.getOutput().streamTo(compressor.getInput());
			compressor.getOutput().streamTo(consumer);

			return StreamConsumers.withResult(serializer.getInput(), consumer.getResult());
		});
	}

	private String tempPath(long chunkId) {
		return chunkId + TEMP_LOG;
	}

	@Override
	public CompletionStage<Void> finish(Set<Long> chunkIds) {
		return client.move(chunkIds.stream().collect(Collectors.toMap(this::tempPath, this::path)));
	}

	@Override
	public CompletionStage<Long> createId() {
		return idGenerator.createId();
	}
}