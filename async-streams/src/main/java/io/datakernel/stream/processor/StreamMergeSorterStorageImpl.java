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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * This class uses for  splitting a single input stream into smaller partitions during merge sort,
 * for avoid overflow RAM, it write it to  external memory . You can write here data with index
 * of partition and then read it from here and merge.
 *
 * @param <T> type of storing data
 */
public final class StreamMergeSorterStorageImpl<T> implements StreamMergeSorterStorage<T> {
	private static final Logger logger = LoggerFactory.getLogger(StreamMergeSorterStorageImpl.class);

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final BufferSerializer<T> serializer;
	private final Path path;
	private final String filePattern;
	private final int blockSize;
	private int partition;

	/**
	 * Creates a new storage
	 *
	 * @param eventloop       event loop in which storage will run
	 * @param executorService executor service for running tasks in new thread
	 * @param serializer      for serialization to bytes
	 * @param path            path in which will store received data
	 * @param blockSize       default buffer size for serializer
	 */
	public StreamMergeSorterStorageImpl(Eventloop eventloop, ExecutorService executorService, BufferSerializer<T> serializer,
	                                    Path path, int blockSize) {
		this.eventloop = checkNotNull(eventloop);
		this.executorService = checkNotNull(executorService);
		this.serializer = checkNotNull(serializer);
		this.path = path.getParent();
		this.filePattern = path.getFileName().toString();
		checkArgument(blockSize >= 0, "blockSize must be positive value,got %s", blockSize);
		this.blockSize = blockSize;
	}

	private Path partitionPath(int i) {
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			logger.error("createDirectories error", e);
		}
		return path.resolve(format(filePattern, i + 1));
	}

	@Override
	public void write(StreamProducer<T> producer, CompletionCallback completionCallback) {
		assert partition >= 0;
		StreamBinarySerializer<T> streamSerializer = new StreamBinarySerializer<>(eventloop, serializer,
				StreamBinarySerializer.MAX_SIZE_2_BYTE, StreamBinarySerializer.MAX_SIZE, 1000, false);
		StreamByteChunker streamByteChunkerBefore = new StreamByteChunker(eventloop, blockSize / 2, blockSize);
		StreamLZ4Compressor streamCompressor = StreamLZ4Compressor.fastCompressor(eventloop);
		StreamByteChunker streamByteChunkerAfter = new StreamByteChunker(eventloop, blockSize / 2, blockSize);
		StreamFileWriter streamWriter = StreamFileWriter.createFile(eventloop, executorService, partitionPath(partition++));

		producer.streamTo(streamSerializer.getInput());
		streamSerializer.getOutput().streamTo(streamByteChunkerBefore.getInput());
		streamByteChunkerBefore.getOutput().streamTo(streamCompressor.getInput());
		streamCompressor.getOutput().streamTo(streamByteChunkerAfter.getInput());
		streamByteChunkerAfter.getOutput().streamTo(streamWriter);
		streamWriter.setFlushCallback(completionCallback);
	}

	/**
	 * Returns producer for reading data from this storage. It read it from external memory,
	 * decompresses and deserializes it
	 *
	 * @param partition index of partition to read
	 */
	@Override
	public StreamProducer<T> read(int partition) {
		assert partition >= 0;

		StreamFileReader streamReader = StreamFileReader.readFileFrom(eventloop, executorService, 1024 * 1024,
				partitionPath(partition), 0L);
		StreamLZ4Decompressor streamDecompressor = new StreamLZ4Decompressor(eventloop);
		StreamBinaryDeserializer<T> streamDeserializer = new StreamBinaryDeserializer<>(eventloop, serializer,
				StreamBinarySerializer.MAX_SIZE);
		streamReader.streamTo(streamDecompressor.getInput());
		streamDecompressor.getOutput().streamTo(streamDeserializer.getInput());
		return streamDeserializer.getOutput();
	}

	/**
	 * Method which removes all creating files
	 */
	@Override
	public void cleanup() {
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
						@Override
						public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
							Files.delete(file);
							return FileVisitResult.CONTINUE;
						}
					});
				} catch (IOException ignored) {
				}
			}
		});

	}

	@Override
	public int nextPartition() {
		return partition;
	}
}
