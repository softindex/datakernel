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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;

import java.net.InetSocketAddress;
import java.util.List;

public class SimpleFsChunkStorage implements AggregationChunkStorage {
	private final Eventloop eventloop;
	private final AggregationStructure structure;
	private final SimpleFsClient client;

	private SimpleFsChunkStorage(Eventloop eventloop, AggregationStructure structure,
	                             InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.structure = structure;
		this.client = SimpleFsClient.create(eventloop, serverAddress);
	}

	public static SimpleFsChunkStorage create(Eventloop eventloop, AggregationStructure structure,
	                                          InetSocketAddress serverAddress) {
		return new SimpleFsChunkStorage(eventloop, structure, serverAddress);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> StreamProducer<T> chunkReader(List<String> keys, List<String> fields,
	                                         Class<T> recordClass, long id, DefiningClassLoader classLoader) {
		final StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);
		BufferSerializer<T> bufferSerializer = structure.createBufferSerializer(recordClass, keys, fields, classLoader);
		StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE);
		decompressor.getOutput().streamTo(deserializer.getInput());

		client.download(path(id), 0, new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> producer) {
				producer.streamTo(decompressor.getInput());
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(decompressor.getInput());
			}
		});

		return deserializer.getOutput();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> void chunkWriter(List<String> keys, List<String> fields, Class<T> recordClass,
	                            final long id, StreamProducer<T> producer, DefiningClassLoader classLoader,
	                            final CompletionCallback callback) {
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		BufferSerializer<T> bufferSerializer = structure.createBufferSerializer(recordClass, keys, fields, classLoader);
		StreamBinarySerializer<T> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE_2_BYTE, StreamBinarySerializer.MAX_SIZE, 1000, false);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(compressor.getInput());
		client.upload(path(id), compressor.getOutput(), new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.onComplete();
			}

			@Override
			public void onException(Exception e) {
				client.delete(path(id), AsyncCallbacks.ignoreCompletionCallback());
				callback.onException(e);
			}
		});
	}

	private String path(long id) {
		return id + ".log";
	}

	@Override
	public void removeChunk(final long id, CompletionCallback callback) {
		client.delete(path(id), callback);
	}
}