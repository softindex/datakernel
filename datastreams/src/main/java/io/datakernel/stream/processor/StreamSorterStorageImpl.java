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

import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serial.file.SerialFileReader;
import io.datakernel.serial.file.SerialFileWriter;
import io.datakernel.serial.processor.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.*;

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
	public static final MemSize DEFAULT_SORTER_BLOCK_SIZE = MemSize.kilobytes(256);

	private static final AtomicInteger PARTITION = new AtomicInteger();

	private final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final ExecutorService executorService;
	private final BufferSerializer<T> serializer;
	private final Path path;

	private String filePattern = DEFAULT_FILE_PATTERN;
	private MemSize readBlockSize = DEFAULT_SORTER_BLOCK_SIZE;
	private MemSize writeBlockSize = DEFAULT_SORTER_BLOCK_SIZE;
	private int compressionLevel = 0;

	// region creators
	private StreamSorterStorageImpl(ExecutorService executorService, BufferSerializer<T> serializer,
			Path path) {
		this.executorService = executorService;
		this.serializer = serializer;
		this.path = path;
	}

	/**
	 * Creates a new storage
	 *
	 * @param executorService executor service for running tasks in new thread
	 * @param serializer      for serialization to bytes
	 * @param path            path in which will store received data
	 */
	public static <T> StreamSorterStorageImpl<T> create(ExecutorService executorService,
			BufferSerializer<T> serializer, Path path) {
		checkArgument(!path.getFileName().toString().contains("%d"));
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return new StreamSorterStorageImpl<>(executorService, serializer, path);
	}

	public StreamSorterStorageImpl<T> withFilePattern(String filePattern) {
		checkArgument(!filePattern.contains("%d"));
		this.filePattern = filePattern;
		return this;
	}

	public StreamSorterStorageImpl<T> withReadBlockSize(MemSize readBlockSize) {
		this.readBlockSize = readBlockSize;
		return this;
	}

	public StreamSorterStorageImpl<T> withWriteBlockSize(MemSize writeBlockSize) {
		this.writeBlockSize = writeBlockSize;
		return this;
	}

	public StreamSorterStorageImpl<T> withCompressionLevel(int compressionLevel) {
		this.compressionLevel = compressionLevel;
		return this;
	}

	// endregion

	private Path partitionPath(int i) {
		return path.resolve(format(filePattern, i));
	}

	@Override
	public Stage<Integer> newPartitionId() {
		return Stage.of(PARTITION.incrementAndGet());
	}

	@Override
	public Stage<StreamConsumer<T>> write(int partition) {
		Path path = partitionPath(partition);
		return AsyncFile.openAsync(executorService, path, new OpenOption[]{WRITE, CREATE_NEW, APPEND})
				.thenApply(file -> StreamConsumer.<T>ofProducer(
						producer -> producer
								.apply(SerialBinarySerializer.create(serializer))
								.apply(SerialByteChunker.create(writeBlockSize.map(bytes -> bytes / 2), writeBlockSize))
								.apply(SerialLZ4Compressor.create(compressionLevel))
								.apply(SerialByteChunker.create(writeBlockSize.map(bytes -> bytes / 2), writeBlockSize))
								.streamTo(SerialFileWriter.create(file)))
						.withLateBinding());
	}

	/**
	 * Returns producer for reading data from this storage. It read it from external memory,
	 * decompresses and deserializes it
	 *
	 * @param partition index of partition to read
	 */
	@Override
	public Stage<StreamProducer<T>> read(int partition) {
		Path path = partitionPath(partition);
		return AsyncFile.openAsync(executorService, path, new OpenOption[]{READ})
				.thenApply(file -> SerialFileReader.readFile(file).withBufferSize(readBlockSize)
						.apply(SerialLZ4Decompressor.create())
						.apply(SerialBinaryDeserializer.create(serializer))
						.withLateBinding());
	}

	/**
	 * Method which removes all creating files
	 */
	@Override
	public Stage<Void> cleanup(List<Integer> partitionsToDelete) {
		return Stage.ofCallable(executorService, () -> {
			for (Integer partitionToDelete : partitionsToDelete) {
				Path path1 = partitionPath(partitionToDelete);
				try {
					Files.delete(path1);
				} catch (IOException e) {
					logger.warn("Could not delete {} : {}", path1, e.toString());
				}
			}
			return null;
		});
	}
}
