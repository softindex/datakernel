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

package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.READ;

/**
 * This class uses for  splitting a single input stream into smaller partitions during merge sort,
 * for avoid overflow RAM, it write it to  external memory . You can write here data with index
 * of partition and then read it from here and merge.
 *
 * @param <T> type of storing data
 */
public final class StreamSorterStorageImpl<T> implements StreamSorterStorage<T> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static final String DEFAULT_FILE_PATTERN = "%d";
	public static final int DEFAULT_SORTER_BLOCK_SIZE = 256 * 1024;

	private static final AtomicInteger PARTITION = new AtomicInteger();

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final BufferSerializer<T> serializer;
	private final Path path;

	private String filePattern = DEFAULT_FILE_PATTERN;
	private int readBlockSize = DEFAULT_SORTER_BLOCK_SIZE;
	private int writeBlockSize = DEFAULT_SORTER_BLOCK_SIZE;
	private int highCompressorLevel = 0;

	// region creators
	private StreamSorterStorageImpl(Eventloop eventloop, ExecutorService executorService, BufferSerializer<T> serializer,
	                                Path path) {
		this.eventloop = eventloop;
		this.executorService = executorService;
		this.serializer = serializer;
		this.path = path;
	}

	/**
	 * Creates a new storage
	 *
	 * @param eventloop       event loop in which storage will run
	 * @param executorService executor service for running tasks in new thread
	 * @param serializer      for serialization to bytes
	 * @param path            path in which will store received data
	 */
	public static <T> StreamSorterStorageImpl<T> create(Eventloop eventloop, ExecutorService executorService,
	                                                    BufferSerializer<T> serializer, Path path) {
		checkArgument(!path.getFileName().toString().contains("%d"));
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return new StreamSorterStorageImpl<>(eventloop, executorService, serializer, path);
	}

	public StreamSorterStorageImpl<T> withFilePattern(String filePattern) {
		checkArgument(!filePattern.contains("%d"));
		this.filePattern = filePattern;
		return this;
	}

	public StreamSorterStorageImpl<T> withReadBlockSize(int readBlockSize) {
		this.readBlockSize = readBlockSize;
		return this;
	}

	public StreamSorterStorageImpl<T> withWriteBlockSize(int writeBlockSize) {
		this.writeBlockSize = writeBlockSize;
		return this;
	}

	public StreamSorterStorageImpl<T> withHighCompressor(int compressorLevel) {
		this.highCompressorLevel = compressorLevel;
		return this;
	}

	// endregion

	private Path partitionPath(int i) {
		return path.resolve(format(filePattern, i));
	}



	@Override
	public CompletionStage<StreamConsumerWithResult<T, Integer>> write() {
		int partition = PARTITION.incrementAndGet();
		Path path = partitionPath(partition);
		return AsyncFile.openAsync(eventloop, executorService, path, StreamFileWriter.CREATE_OPTIONS).thenApply(file -> {
			StreamBinarySerializer<T> streamSerializer = StreamBinarySerializer.create(eventloop, serializer);
			StreamByteChunker streamByteChunkerBefore = StreamByteChunker.create(eventloop, writeBlockSize / 2, writeBlockSize);
			StreamLZ4Compressor streamCompressor = highCompressorLevel == 0 ?
					StreamLZ4Compressor.fastCompressor(eventloop) :
					StreamLZ4Compressor.highCompressor(eventloop, highCompressorLevel);
			StreamByteChunker streamByteChunkerAfter = StreamByteChunker.create(eventloop, writeBlockSize / 2, writeBlockSize);
			StreamFileWriter streamWriter = StreamFileWriter.create(eventloop, file);

			streamSerializer.getOutput().streamTo(streamByteChunkerBefore.getInput());
			streamByteChunkerBefore.getOutput().streamTo(streamCompressor.getInput());
			streamCompressor.getOutput().streamTo(streamByteChunkerAfter.getInput());
			streamByteChunkerAfter.getOutput().streamTo(streamWriter);

			return StreamConsumerWithResult.create(streamSerializer.getInput(), streamWriter.getFlushStage().thenApply($ -> partition));
		});
	}

	/**
	 * Returns producer for reading data from this storage. It read it from external memory,
	 * decompresses and deserializes it
	 *
	 * @param partition index of partition to read
	 */
	@Override
	public CompletionStage<StreamProducerWithResult<T, Void>> read(int partition) {
		Path path = partitionPath(partition);
		return AsyncFile.openAsync(eventloop, executorService, path, new OpenOption[]{READ})
				.thenApply(file -> {
					StreamFileReader fileReader = StreamFileReader.readFileFrom(eventloop, file, readBlockSize, 0L);
					StreamLZ4Decompressor streamDecompressor = StreamLZ4Decompressor.create(eventloop);
					StreamBinaryDeserializer<T> streamDeserializer = StreamBinaryDeserializer.create(eventloop, serializer);

					fileReader.streamTo(streamDecompressor.getInput());
					streamDecompressor.getOutput().streamTo(streamDeserializer.getInput());

					return StreamProducerWithResult.wrap(streamDeserializer.getOutput());
				});
	}

	/**
	 * Method which removes all creating files
	 */
	@Override
	public CompletionStage<Void> cleanup(final List<Integer> partitionsToDelete) {
		return eventloop.runConcurrently(executorService, () -> {
			for (Integer partitionToDelete : partitionsToDelete) {
				Path path = partitionPath(partitionToDelete);
				try {
					Files.delete(path);
				} catch (IOException e) {
					logger.warn("Could not delete {} : {}", path, e.toString());
				}
			}
		});
	}
}
