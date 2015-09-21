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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

@SuppressWarnings("unchecked")
public class SimpleFsAggregationStorage implements AggregationChunkStorage {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsAggregationStorage.class);

	private final NioEventloop eventloop;
	private final AggregationStructure aggregationStructure;
	private final SimpleFsClient client;

	private static final int DEFAULT_LISTEN_PORT = 45555;
	private static final InetSocketAddress DEFAULT_SERVER_ADDRESS = new InetSocketAddress("127.0.0.1", DEFAULT_LISTEN_PORT);

	private final InetSocketAddress serverAddress;

	public SimpleFsAggregationStorage(NioEventloop eventloop, AggregationStructure aggregationStructure) {
		this(eventloop, aggregationStructure, DEFAULT_SERVER_ADDRESS);
	}

	public SimpleFsAggregationStorage(NioEventloop eventloop, AggregationStructure aggregationStructure, InetSocketAddress serverAddress) {
		this.eventloop = eventloop;
		this.aggregationStructure = aggregationStructure;
		this.client = new SimpleFsClient(eventloop);
		this.serverAddress = serverAddress;
	}

	@Override
	public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> keys, List<String> fields,
	                                         Class<T> recordClass, final long id) {
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, keys, fields);
		StreamBinaryDeserializer<T> deserializer = new StreamBinaryDeserializer<>(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE);

		final StreamForwarder forwarder = new StreamForwarder<>(eventloop);

		forwarder.streamTo(deserializer);

		client.read(serverAddress, path(id), new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> result) {
				result.streamTo(forwarder);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Opening stream for reading chunk #{} from SimpleFS failed.", id, exception);
				StreamProducers.closingWithError(eventloop, exception).streamTo(forwarder);
			}
		});

		return deserializer;
	}

	@Override
	public <T> StreamConsumer<T> chunkWriter(String aggregationId, final List<String> keys, final List<String> fields,
	                                         final Class<T> recordClass, final long id) {
		BufferSerializer<T> bufferSerializer = aggregationStructure.createBufferSerializer(recordClass, keys, fields);
		StreamBinarySerializer<T> serializer = new StreamBinarySerializer<>(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE, StreamBinarySerializer.MAX_SIZE, 1000, false);

		final StreamForwarder forwarder = new StreamForwarder<>(eventloop);

		serializer.streamTo(forwarder);

		client.write(serverAddress, path(id), new ResultCallback<StreamConsumer<ByteBuf>>() {
			@Override
			public void onResult(StreamConsumer<ByteBuf> result) {
				forwarder.streamTo(result);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Opening stream for writing chunk #{} to SimpleFS failed.", id, exception);
				forwarder.streamTo(StreamConsumers.closingWithError(eventloop, exception));
			}
		});

		return serializer;
	}

	private String path(long id) {
		return id + ".log";
	}

	@Override
	public void removeChunk(final String aggregationId, final long id) {
		client.deleteFile(serverAddress, path(id), new ResultCallback<Boolean>() {
			@Override
			public void onResult(Boolean result) {
				logger.trace("Removing chunk #{} completed successfully.", id);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Removing chunk #{} from SimpleFS failed.", id);
			}
		});
	}
}
