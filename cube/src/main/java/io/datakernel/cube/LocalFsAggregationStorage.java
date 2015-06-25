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

package io.datakernel.cube;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Stores aggregation chunks in local file system.
 */
public class LocalFsAggregationStorage implements AggregationStorage {
	private static final Logger logger = getLogger(LocalFsAggregationStorage.class);

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final CubeStructure cubeStructure;

	private final Path dir;

	/**
	 * Constructs an aggregation storage, that runs in the specified event loop, performs blocking IO in the given executor,
	 * serializes records according to specified cube structure and stores data in the given directory.
	 *
	 * @param eventloop       event loop, in which aggregation storage is to run
	 * @param executorService executor, where blocking IO operations are to be run
	 * @param cubeStructure   structure of a cube, whose aggregations chunks are to be persisted
	 * @param dir             directory where data is saved
	 */
	public LocalFsAggregationStorage(Eventloop eventloop, ExecutorService executorService,
	                                 CubeStructure cubeStructure,
	                                 Path dir) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.cubeStructure = cubeStructure;
		this.dir = dir;
	}

	private Path path(String aggregationId, long id) {
		Path tableDir = dir.resolve(aggregationId);
		Path path = tableDir.resolve(id + ".log");
		try {
			Files.createDirectories(tableDir);
		} catch (IOException e) {
			logger.error("createDirectories error", e);
		}
		return path;
	}

	@Override
	public void removeChunk(String aggregationId, long id) {
		Path path = path(aggregationId, id);
		try {
			Files.delete(path);
		} catch (IOException e) {
			logger.error("delete error", e);
		}
	}

	@Override
	public <T> StreamProducer<T> chunkReader(String aggregationId, List<String> dimensions, List<String> measures, Class<T> recordClass, long id) {

		StreamProducer<ByteBuf> streamFileReader = StreamFileReader.readFileFrom(eventloop, executorService, 256 * 1024,
				path(aggregationId, id), 0L);

		StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		BufferSerializer<T> bufferSerializer = cubeStructure.createBufferSerializer(recordClass, dimensions, measures);
		StreamBinaryDeserializer<T> deserializer = new StreamBinaryDeserializer<>(eventloop, bufferSerializer, StreamBinarySerializer.MAX_SIZE);

		streamFileReader.streamTo(decompressor);
		decompressor.streamTo(deserializer);

		return deserializer;
	}

	@Override
	public <T> StreamConsumer<T> chunkWriter(String aggregationId, List<String> dimensions, List<String> measures, Class<T> recordClass, long id) {
		BufferSerializer<T> bufferSerializer = cubeStructure.createBufferSerializer(recordClass, dimensions, measures);
		StreamBinarySerializer<T> serializer = new StreamBinarySerializer<>(eventloop, bufferSerializer, StreamBinarySerializer.MAX_SIZE, StreamBinarySerializer.MAX_SIZE, 1000, false);
		StreamLZ4Compressor compressor = new StreamLZ4Compressor(eventloop, 1024 * 1024, 256 * 1024);
		StreamFileWriter writer = StreamFileWriter.createFile(eventloop, executorService, path(aggregationId, id));

		serializer.streamTo(compressor);
		compressor.streamTo(writer);

		return serializer;
	}

}
