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
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.READ;

/**
 * This class uses for  splitting a single input stream into smaller partitions during merge sort,
 * for avoid overflow RAM, it write it to  external memory . You can write here data with index
 * of partition and then read it from here and merge.
 *
 * @param <T> type of storing data
 */
public final class StreamMergeSorterStorageImpl<T> implements StreamMergeSorterStorage<T> {
	private static final AtomicInteger PARTITION = new AtomicInteger();

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Eventloop eventloop;
	private final ExecutorService executorService;
	private final BufferSerializer<T> serializer;
	private final Path path;
	private final String filePattern;
	private final int blockSize;

	// region creators
	private StreamMergeSorterStorageImpl(Eventloop eventloop, ExecutorService executorService, BufferSerializer<T> serializer,
	                                     Path path, int blockSize) {
		this.eventloop = checkNotNull(eventloop);
		this.executorService = checkNotNull(executorService);
		this.serializer = checkNotNull(serializer);
		this.path = path.getParent();
		this.filePattern = path.getFileName().toString();
		checkArgument(blockSize >= 0, "blockSize must be positive value,got %s", blockSize);
		this.blockSize = blockSize;
	}

	/**
	 * Creates a new storage
	 *
	 * @param eventloop       event loop in which storage will run
	 * @param executorService executor service for running tasks in new thread
	 * @param serializer      for serialization to bytes
	 * @param path            path in which will store received data
	 * @param blockSize       default buffer size for serializer
	 */
	public static <T> StreamMergeSorterStorageImpl<T> create(Eventloop eventloop, ExecutorService executorService,
	                                                         BufferSerializer<T> serializer, Path path, int blockSize) {
		return new StreamMergeSorterStorageImpl<T>(eventloop, executorService, serializer, path, blockSize);
	}
	// endregion

	private Path partitionPath(int i) {
		try {
			Files.createDirectories(path);
		} catch (IOException e) {
			logger.error("createDirectories error", e);
		}
		return path.resolve(format(filePattern, i));
	}

	@Override
	public int write(final StreamProducer<T> producer, final CompletionCallback completionCallback) {
		StreamBinarySerializer<T> streamSerializer = StreamBinarySerializer.create(eventloop, serializer,
				StreamBinarySerializer.MAX_SIZE_2_BYTE, StreamBinarySerializer.MAX_SIZE, 1000, false);
		StreamByteChunker streamByteChunkerBefore = StreamByteChunker.create(eventloop, blockSize / 2, blockSize);
		StreamLZ4Compressor streamCompressor = StreamLZ4Compressor.fastCompressor(eventloop);
		final StreamByteChunker streamByteChunkerAfter = StreamByteChunker.create(eventloop, blockSize / 2, blockSize);

		producer.streamTo(streamSerializer.getInput());
		streamSerializer.getOutput().streamTo(streamByteChunkerBefore.getInput());
		streamByteChunkerBefore.getOutput().streamTo(streamCompressor.getInput());
		streamCompressor.getOutput().streamTo(streamByteChunkerAfter.getInput());

		int partition = PARTITION.incrementAndGet();
		AsyncFile.open(eventloop, executorService, partitionPath(partition), StreamFileWriter.CREATE_OPTIONS,
				new ForwardingResultCallback<AsyncFile>(completionCallback) {
					@Override
					public void onResult(AsyncFile file) {
						StreamFileWriter streamWriter = StreamFileWriter.create(eventloop, file);
						streamByteChunkerAfter.getOutput().streamTo(streamWriter);
						streamWriter.setFlushCallback(completionCallback);
					}
				});
		return partition;
	}

	/**
	 * Returns producer for reading data from this storage. It read it from external memory,
	 * decompresses and deserializes it
	 *
	 * @param partition index of partition to read
	 */
	@Override
	public StreamProducer<T> read(int partition, final CompletionCallback callback) {
		assert partition >= 0;

		final StreamLZ4Decompressor streamDecompressor = StreamLZ4Decompressor.create(eventloop);
		StreamBinaryDeserializer<T> streamDeserializer = StreamBinaryDeserializer.create(eventloop, serializer,
				StreamBinarySerializer.MAX_SIZE);
		streamDecompressor.getOutput().streamTo(streamDeserializer.getInput());

		AsyncFile.open(eventloop, executorService, partitionPath(partition), new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
			@Override
			public void onResult(AsyncFile file) {
				StreamFileReader fileReader = StreamFileReader.readFileFrom(eventloop, file, 1024 * 1024, 0L);
				fileReader.streamTo(streamDecompressor.getInput());
				fileReader.setPositionCallback(new ResultCallback<Long>() {
					@Override
					public void onResult(Long position) {
						callback.onComplete();
					}

					@Override
					public void onException(Exception e) {
						callback.onException(e);
					}
				});
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(streamDecompressor.getInput());
			}
		});

		return streamDeserializer.getOutput();
	}

	/**
	 * Method which removes all creating files
	 */
	@Override
	public void cleanup(final List<Integer> partitionsToDelete) {
		try {
			executorService.execute(new Runnable() {
				@Override
				public void run() {
					try {
						Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
							@Override
							public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
								for (Integer partitionToDelete : partitionsToDelete) {
									if (file.getFileName().toString().equals(format(filePattern, partitionToDelete))) {
										Files.delete(file);
									}
								}
								return FileVisitResult.CONTINUE;
							}
						});
					} catch (IOException e) {
						logger.error("Exception occurred while trying to perform cleanup", e);
					}
				}
			});
		} catch (RejectedExecutionException e) {
			logger.error("Cleanup task has been rejected", e);
		}
	}
}
