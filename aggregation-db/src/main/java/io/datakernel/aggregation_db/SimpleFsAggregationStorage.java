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

package io.datakernel.aggregation_db;

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

public class SimpleFsAggregationStorage implements AggregationChunkStorage {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsAggregationStorage.class);

	private final NioEventloop eventloop;
	private final AggregationStructure aggregationStructure;
	private final SimpleFsClient client;

	private static final int LISTEN_PORT = 45555;
	private static final InetSocketAddress address = new InetSocketAddress("127.0.0.1", LISTEN_PORT);

	public SimpleFsAggregationStorage(NioEventloop eventloop, AggregationStructure aggregationStructure) {
		this(eventloop, aggregationStructure, address);
	}

	public SimpleFsAggregationStorage(NioEventloop eventloop, AggregationStructure aggregationStructure, InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.aggregationStructure = aggregationStructure;
		this.client = SimpleFsClient.createInstance(eventloop, serverAddress);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> dimensions, List<String> measures,
	                                         Class<T> recordClass, final long id) {
		StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, dimensions, measures);
		StreamBinaryDeserializer<T> deserializer = new StreamBinaryDeserializer<>(eventloop, bufferSerializer, StreamBinarySerializer.MAX_SIZE);
		decompressor.getOutput().streamTo(deserializer.getInput());

		client.download(path(id), decompressor.getInput());

		return deserializer.getOutput();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> StreamConsumer<T> chunkWriter(String aggregationId, final List<String> dimensions, final List<String> measures,
	                                         final Class<T> recordClass, final long id, final CompletionCallback callback) {
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, dimensions, measures);
		StreamBinarySerializer<T> serializer = new StreamBinarySerializer<>(eventloop, bufferSerializer, StreamBinarySerializer.MAX_SIZE, StreamBinarySerializer.MAX_SIZE, 1000, false);
		serializer.getOutput().streamTo(compressor.getInput());

		client.upload(path(id), compressor.getOutput(), new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Uploaded chunk #{} to SimpleFS successfully", id);
				callback.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Uploading chunk #{} to SimpleFS failed", id, exception);
				callback.onException(exception);
			}
		});

		return serializer.getInput();
	}

	private String path(long id) {
		return id + ".log";
	}

	@Override
	public void removeChunk(final String aggregationId, final long id, CompletionCallback callback) {
		client.delete(path(id), callback);
	}
}