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

import io.datakernel.async.AsyncExecutor;
import io.datakernel.async.AsyncTask;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

public class SimpleFsChunkStorage implements AggregationChunkStorage {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsChunkStorage.class);

	private final Eventloop eventloop;
	private final AggregationStructure aggregationStructure;
	private final SimpleFsClient client;
	private final AsyncExecutor asyncExecutor;

	public SimpleFsChunkStorage(Eventloop eventloop, AggregationStructure aggregationStructure,
	                            AsyncExecutor asyncExecutor, InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.aggregationStructure = aggregationStructure;
		this.asyncExecutor = asyncExecutor;
		this.client = SimpleFsClient.createInstance(eventloop, serverAddress);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> keys, List<String> fields,
	                                         Class<T> recordClass, final long id) {
		StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, keys, fields);
		StreamBinaryDeserializer<T> deserializer = new StreamBinaryDeserializer<>(eventloop, bufferSerializer, StreamBinarySerializer.MAX_SIZE);
		decompressor.getOutput().streamTo(deserializer.getInput());

		client.download(path(id), decompressor.getInput());

		return deserializer.getOutput();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> void chunkWriter(String aggregationId, List<String> keys, List<String> fields, Class<T> recordClass,
	                            final long id, final StreamProducer<T> producer, final CompletionCallback callback) {
		final StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, keys, fields);
		final StreamBinarySerializer<T> serializer = new StreamBinarySerializer<>(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE, StreamBinarySerializer.MAX_SIZE, 1000, false);

		asyncExecutor.submit(new AsyncTask() {
			@Override
			public void execute(CompletionCallback callback) {
				producer.streamTo(serializer.getInput());
				serializer.getOutput().streamTo(compressor.getInput());
				client.upload(path(id), compressor.getOutput(), callback);
			}
		}, new CompletionCallback() {
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
	}

	private String path(long id) {
		return id + ".log";
	}

	@Override
	public void removeChunk(final String aggregationId, final long id, CompletionCallback callback) {
		client.delete(path(id), callback);
	}
}