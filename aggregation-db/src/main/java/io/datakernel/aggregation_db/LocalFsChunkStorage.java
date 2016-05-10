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
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.READ;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Stores aggregation chunks in local file system.
 */
public class LocalFsChunkStorage implements AggregationChunkStorage {
	private static final Logger logger = getLogger(LocalFsChunkStorage.class);

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final AggregationStructure structure;

	private final Path dir;

	/**
	 * Constructs an aggregation storage, that runs in the specified event loop, performs blocking IO in the given executor,
	 * serializes records according to specified aggregation structure and stores data in the given directory.
	 *
	 * @param eventloop       event loop, in which aggregation storage is to run
	 * @param executorService executor, where blocking IO operations are to be run
	 * @param dir             directory where data is saved
	 */
	public LocalFsChunkStorage(Eventloop eventloop, ExecutorService executorService, AggregationStructure structure,
	                           Path dir) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.structure = structure;
		this.dir = dir;
	}

	private Path path(long id) {
		try {
			Files.createDirectories(dir);
		} catch (IOException e) {
			logger.error("createDirectories error", e);
		}
		return dir.resolve(id + ".log");
	}

	@Override
	public void removeChunk(long id, CompletionCallback callback) {
		Path path = path(id);
		try {
			Files.delete(path);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("delete error", e);
			callback.onException(e);
		}
	}

	@Override
	public <T> StreamProducer<T> chunkReader(List<String> keys, List<String> fields,
	                                         Class<T> recordClass, long id, DefiningClassLoader classLoader) {
		final StreamLZ4Decompressor decompressor = new StreamLZ4Decompressor(eventloop);
		BufferSerializer<T> bufferSerializer = structure.createBufferSerializer(recordClass, keys, fields, classLoader);
		StreamBinaryDeserializer<T> deserializer = new StreamBinaryDeserializer<>(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE);
		decompressor.getOutput().streamTo(deserializer.getInput());

		AsyncFile.open(eventloop, executorService, path(id), new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
			@Override
			public void onResult(AsyncFile file) {
				StreamFileReader fileReader = StreamFileReader.readFileFrom(eventloop, file, 1024 * 1024, 0L);
				fileReader.streamTo(decompressor.getInput());
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(decompressor.getInput());
			}
		});

		return deserializer.getOutput();
	}

	@Override
	public <T> void chunkWriter(List<String> keys, List<String> fields, Class<T> recordClass, long id,
	                            StreamProducer<T> producer, DefiningClassLoader classLoader,
	                            final CompletionCallback callback) {
		final StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		BufferSerializer<T> bufferSerializer = structure.createBufferSerializer(recordClass, keys, fields, classLoader);
		StreamBinarySerializer<T> serializer = new StreamBinarySerializer<>(eventloop, bufferSerializer,
				StreamBinarySerializer.MAX_SIZE_2_BYTE, StreamBinarySerializer.MAX_SIZE, 1000, false);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(compressor.getInput());

		AsyncFile.open(eventloop, executorService, path(id), StreamFileWriter.CREATE_OPTIONS,
				new ForwardingResultCallback<AsyncFile>(callback) {
					@Override
					public void onResult(AsyncFile file) {
						StreamFileWriter writer = StreamFileWriter.create(eventloop, file, true);
						compressor.getOutput().streamTo(writer);
						writer.setFlushCallback(callback);
					}
				});
	}
}
