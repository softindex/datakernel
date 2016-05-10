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

import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
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
	private final AggregationStructure aggregationStructure;
	private final SimpleFsClient client;

	public SimpleFsChunkStorage(Eventloop eventloop, AggregationStructure aggregationStructure,
	                            InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.aggregationStructure = aggregationStructure;
		this.client = new SimpleFsClient(eventloop, serverAddress);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> StreamProducer<T> chunkReader(List<String> keys, List<String> fields,
	                                         Class<T> recordClass, final long id) {
		final StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, keys, fields);
		StreamBinaryDeserializer<T> deserializer = new StreamBinaryDeserializer<>(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE);
		decompressor.getOutput().streamTo(deserializer.getInput());

		client.download(path(id), 0, new ResultCallback<StreamTransformerWithCounter>() {
			@Override
			public void onResult(StreamTransformerWithCounter result) {
				result.getOutput().streamTo(decompressor.getInput());
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
	                            final long id, final StreamProducer<T> producer, final CompletionCallback callback) {
		StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, keys, fields);
		StreamBinarySerializer<T> serializer = new StreamBinarySerializer<>(eventloop, bufferSerializer,
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