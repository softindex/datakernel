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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import io.datakernel.util.MemSize;
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
	private final Logger logger = getLogger(this.getClass());

	private final Eventloop eventloop;
	private final ExecutorService executorService;

	private final Path dir;

	private int bufferSize = 1024 * 1024;

	/**
	 * Constructs an aggregation storage, that runs in the specified event loop, performs blocking IO in the given executor,
	 * serializes records according to specified aggregation structure and stores data in the given directory.
	 *
	 * @param eventloop       event loop, in which aggregation storage is to run
	 * @param executorService executor, where blocking IO operations are to be run
	 * @param dir             directory where data is saved
	 */
	private LocalFsChunkStorage(Eventloop eventloop, ExecutorService executorService, Path dir) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.dir = dir;
	}

	public static LocalFsChunkStorage create(Eventloop eventloop, ExecutorService executorService, Path dir) {
		return new LocalFsChunkStorage(eventloop, executorService, dir);
	}

	public LocalFsChunkStorage withBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public LocalFsChunkStorage withBufferSize(MemSize bufferSize) {
		this.bufferSize = (int) bufferSize.get();
		return this;
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
			callback.setComplete();
		} catch (IOException e) {
			logger.error("delete error", e);
			callback.setException(e);
		}
	}

	@Override
	public <T> void read(final Aggregation aggregation, final List<String> keys, final List<String> fields,
	                     final Class<T> recordClass, long id, final DefiningClassLoader classLoader,
	                     final ResultCallback<StreamProducer<T>> callback) {
		AsyncFile.open(eventloop, executorService, path(id), new OpenOption[]{READ}, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			protected void onResult(AsyncFile file) {
				StreamFileReader fileReader = StreamFileReader.readFileFrom(eventloop, file, bufferSize, 0L);

				StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop);
				fileReader.streamTo(decompressor.getInput());

				BufferSerializer<T> bufferSerializer = AggregationUtils.createBufferSerializer(aggregation, recordClass,
						keys, fields, classLoader);
				StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);

				decompressor.getOutput().streamTo(deserializer.getInput());
				callback.setResult(deserializer.getOutput());
			}
		});
	}

	@Override
	public <T> void write(StreamProducer<T> producer, Aggregation aggregation, List<String> keys, List<String> fields,
	                      Class<T> recordClass, long id,
	                      DefiningClassLoader classLoader,
	                      final CompletionCallback callback) {
		final StreamLZ4Compressor compressor = StreamLZ4Compressor.fastCompressor(eventloop);
		BufferSerializer<T> bufferSerializer = AggregationUtils.createBufferSerializer(aggregation, recordClass,
				keys, fields, classLoader);
		StreamBinarySerializer<T> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer)
				.withDefaultBufferSize(StreamBinarySerializer.MAX_SIZE_2)
				.withFlushDelay(1000);

		producer.streamTo(serializer.getInput());
		serializer.getOutput().streamTo(compressor.getInput());

		AsyncFile.open(eventloop, executorService, path(id), StreamFileWriter.CREATE_OPTIONS,
				new ForwardingResultCallback<AsyncFile>(callback) {
					@Override
					protected void onResult(AsyncFile file) {
						StreamFileWriter writer = StreamFileWriter.create(eventloop, file, true);
						compressor.getOutput().streamTo(writer);
						writer.setFlushCallback(callback);
					}
				});
	}
}
