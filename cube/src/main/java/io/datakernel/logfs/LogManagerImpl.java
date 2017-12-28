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

package io.datakernel.logfs;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import io.datakernel.stream.processor.StreamLZ4Decompressor;
import io.datakernel.util.MemSize;
import io.datakernel.util.Preconditions;
import io.datakernel.util.Stopwatch;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static io.datakernel.stream.StreamProducers.endOfStreamOnError;
import static io.datakernel.stream.StreamProducers.onEndOfStream;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class LogManagerImpl<T> implements LogManager<T> {
	public static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH").withZone(DateTimeZone.UTC);
	public static final int DEFAULT_BUFFER_SIZE = 256 * 1024;
	public static final int DEFAULT_AUTO_FLUSH_INTERVAL = -1;

	private final Logger logger = LoggerFactory.getLogger(LogManagerImpl.class);
	private final Eventloop eventloop;
	private final LogFileSystem fileSystem;
	private final BufferSerializer<T> serializer;
	private final DateTimeFormatter dateTimeFormatter;

	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private int autoFlushIntervalMillis = DEFAULT_AUTO_FLUSH_INTERVAL;

	private LogManagerImpl(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer) {
		this(eventloop, fileSystem, serializer, DEFAULT_DATE_TIME_FORMATTER);
	}

	private LogManagerImpl(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer,
	                       DateTimeFormatter dateTimeFormatter) {
		this.eventloop = eventloop;
		this.fileSystem = fileSystem;
		this.serializer = serializer;
		this.dateTimeFormatter = dateTimeFormatter;
	}

	public static <T> LogManagerImpl<T> create(Eventloop eventloop, LogFileSystem fileSystem,
	                                           BufferSerializer<T> serializer) {
		return new LogManagerImpl<>(eventloop, fileSystem, serializer);
	}

	public static <T> LogManagerImpl<T> create(Eventloop eventloop, LogFileSystem fileSystem,
	                                           BufferSerializer<T> serializer, DateTimeFormatter dateTimeFormatter) {
		return new LogManagerImpl<>(eventloop, fileSystem, serializer, dateTimeFormatter);
	}

	public LogManagerImpl<T> withBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public LogManagerImpl<T> withBufferSize(MemSize bufferSize) {
		this.bufferSize = (int) bufferSize.get();
		return this;
	}

	public LogManagerImpl<T> withAutoDelay(int autoFlushIntervalMillis) {
		this.autoFlushIntervalMillis = autoFlushIntervalMillis;
		return this;
	}

	@Override
	public CompletionStage<StreamConsumerWithResult<T, Void>> consumer(String logPartition) {
		validateLogPartition(logPartition);
		return Stages.ofSupplier(() -> {
			StreamBinarySerializer<T> streamBinarySerializer = StreamBinarySerializer.create(eventloop, serializer)
					.withAutoFlush(autoFlushIntervalMillis)
					.withDefaultBufferSize(bufferSize)
					.withSkipSerializationErrors();

			StreamLZ4Compressor streamCompressor = StreamLZ4Compressor.fastCompressor(eventloop);

			LogStreamChunker writer = LogStreamChunker.create(eventloop, fileSystem, dateTimeFormatter, logPartition);

			streamBinarySerializer.getOutput().streamTo(streamCompressor.getInput());
			streamCompressor.getOutput().streamTo(writer);

			return StreamConsumers.withResult(streamBinarySerializer.getInput(), writer.getResult());
		});
	}

	@Override
	public CompletionStage<StreamProducerWithResult<T, LogPosition>> producer(String logPartition,
	                                                                          LogFile startLogFile, long startOffset,
	                                                                          LogFile endLogFile) {
		validateLogPartition(logPartition);
		LogPosition startPosition = LogPosition.create(startLogFile, startOffset);
		return fileSystem.list(logPartition).thenApply(logFiles -> {
			List<LogFile> logFilesToRead = logFiles.stream().filter(logFile -> isFileInRange(logFile, startPosition, endLogFile)).collect(Collectors.toList());
			Collections.sort(logFilesToRead);

			Iterator<LogFile> it = logFilesToRead.iterator();
			SettableStage<LogPosition> positionStage = SettableStage.create();

			return StreamProducers.withResult(StreamProducers.concat(eventloop, new Iterator<StreamProducer<T>>() {
				private int n;

				private LogFile currentLogFile;
				private long inputStreamPosition;

				final Stopwatch sw = Stopwatch.createUnstarted();

				@Override
				public boolean hasNext() {
					if (it.hasNext()) return true;
					positionStage.set(getLogPosition());
					return false;
				}

				public LogPosition getLogPosition() {
					if (currentLogFile == null)
						return startPosition;

					if (currentLogFile.equals(startPosition.getLogFile()))
						return LogPosition.create(currentLogFile, startPosition.getPosition() + inputStreamPosition);

					return LogPosition.create(currentLogFile, inputStreamPosition);
				}

				@Override
				public StreamProducer<T> next() {
					currentLogFile = it.next();
					final long position = n++ == 0 ? startPosition.getPosition() : 0L;

					if (logger.isTraceEnabled()) logger.trace("Read log file `{}` from: {}", currentLogFile, position);

					CompletionStage<StreamProducer<T>> stage = fileSystem.read(logPartition, currentLogFile, position)
							.thenApply(producer -> {
								inputStreamPosition = 0L;

								sw.reset().start();
								StreamLZ4Decompressor decompressor = StreamLZ4Decompressor.create(eventloop)
										.withInspector(new StreamLZ4Decompressor.Inspector() {
											@Override
											public void onInputBuf(StreamLZ4Decompressor self, ByteBuf buf) {
											}

											@Override
											public void onBlock(StreamLZ4Decompressor self, StreamLZ4Decompressor.Header header, ByteBuf inputBuf, ByteBuf outputBuf) {
												inputStreamPosition += StreamLZ4Decompressor.HEADER_LENGTH + header.compressedLen;
											}
										});
								StreamBinaryDeserializer<T> deserializer = StreamBinaryDeserializer.create(eventloop, serializer);

								producer.streamTo(decompressor.getInput());
								decompressor.getOutput().streamTo(deserializer.getInput());

								return onEndOfStream(endOfStreamOnError(deserializer.getOutput()), this::log);
							});

					return StreamProducers.ofStage(stage);
				}

				private void log(Void aVoid, Throwable throwable) {
					if (throwable == null && logger.isTraceEnabled()) {
						logger.trace("Finish log file `{}` in {}, compressed bytes: {} ({} bytes/ses)", currentLogFile,
								sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1));
					} else if (throwable != null && logger.isWarnEnabled()) {
						logger.warn("Error on log file `{}` in {}, compressed bytes: {} ({} bytes/ses)", currentLogFile,
								sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1), throwable);
					}
				}
			}), positionStage);
		});
	}

	private static void validateLogPartition(String logPartition) {
		Preconditions.checkArgument(!logPartition.contains("-"), "Using dash (-) in log partition name is not allowed");
	}

	private boolean isFileInRange(LogFile logFile, LogPosition startPosition, @Nullable LogFile endFile) {
		if (startPosition.getLogFile() != null && logFile.compareTo(startPosition.getLogFile()) < 0)
			return false;

		if (endFile != null && logFile.compareTo(endFile) > 0)
			return false;

		return true;
	}

	public DateTimeFormatter getDateTimeFormatter() {
		return dateTimeFormatter;
	}
}
