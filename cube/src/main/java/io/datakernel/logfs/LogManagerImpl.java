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
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.serial.processor.SerialLZ4Compressor;
import io.datakernel.serial.processor.SerialLZ4Decompressor;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.util.MemSize;
import io.datakernel.util.Preconditions;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class LogManagerImpl<T> implements LogManager<T>, EventloopJmxMBeanEx {
	public static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH").withZone(ZoneOffset.UTC);
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

	private final Logger logger = LoggerFactory.getLogger(LogManagerImpl.class);
	private final Eventloop eventloop;
	private final LogFileSystem fileSystem;
	private final BufferSerializer<T> serializer;
	private final DateTimeFormatter dateTimeFormatter;

	private MemSize bufferSize = DEFAULT_BUFFER_SIZE;
	private Duration autoFlushInterval = null;

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
		this.bufferSize = MemSize.of(bufferSize);
		return this;
	}

	public LogManagerImpl<T> withBufferSize(MemSize bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public LogManagerImpl<T> withAutoFlushInterval(Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		return this;
	}

	@Override
	public Stage<StreamConsumer<T>> consumer(String logPartition) {
		validateLogPartition(logPartition);

		return Stage.of(StreamConsumer.<T>ofProducer(
				producer -> producer
						.apply(SerialBinarySerializer.create(serializer)
								.withAutoFlushInterval(autoFlushInterval)
								.withInitialBufferSize(bufferSize)
								.withSkipSerializationErrors())
						.apply(SerialLZ4Compressor.createFastCompressor())
						.streamTo(LogStreamChunker.create(fileSystem, dateTimeFormatter, logPartition)))
				.withLateBinding());
	}

	@Override
	public Stage<StreamProducerWithResult<T, LogPosition>> producer(String logPartition,
			LogFile startLogFile, long startOffset,
			LogFile endLogFile) {
		validateLogPartition(logPartition);
		LogPosition startPosition = LogPosition.create(startLogFile, startOffset);
		SettableStage<LogPosition> positionStage = new SettableStage<>();
		SettableStage<StreamProducerWithResult<T, LogPosition>> resultStage = new SettableStage<>();
		fileSystem.list(logPartition)
				.whenResult(logFiles -> {
					List<LogFile> logFilesToRead = logFiles.stream().filter(logFile -> isFileInRange(logFile, startPosition, endLogFile)).collect(Collectors.toList());
					Collections.sort(logFilesToRead);

					Iterator<LogFile> it = logFilesToRead.iterator();

					Iterator<StreamProducer<T>> producers = new Iterator<StreamProducer<T>>() {
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
							long position = n++ == 0 ? startPosition.getPosition() : 0L;

							if (logger.isTraceEnabled())
								logger.trace("Read log file `{}` from: {}", currentLogFile, position);

							Stage<StreamProducer<T>> stage = fileSystem.read(logPartition, currentLogFile, position).thenApply(fileStream -> {
								inputStreamPosition = 0L;
								sw.reset().start();
								return fileStream
										.apply(SerialLZ4Decompressor.create()
												.withInspector(new SerialLZ4Decompressor.Inspector() {
													@Override
													public void onInputBuf(SerialLZ4Decompressor self, ByteBuf buf) {
													}

													@Override
													public void onBlock(SerialLZ4Decompressor self, SerialLZ4Decompressor.Header header, ByteBuf inputBuf, ByteBuf outputBuf) {
														inputStreamPosition += SerialLZ4Decompressor.HEADER_LENGTH + header.compressedLen;
													}
												}))
//										.with(endOfStreamOnError(throwable -> throwable instanceof TruncatedDataException)) // TODO
										.apply(SerialBinaryDeserializer.create(serializer))
										.thenRun(() -> log(null))
										.whenException(this::log)
										.withLateBinding();
							});

							return StreamProducer.ofStage(stage);
						}

						private void log(Throwable throwable) {
							if (throwable == null && logger.isTraceEnabled()) {
								logger.trace("Finish log file `{}` in {}, compressed bytes: {} ({} bytes/s)", currentLogFile,
										sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1));
							} else if (throwable != null && logger.isErrorEnabled()) {
								logger.error("Error on log file `{}` in {}, compressed bytes: {} ({} bytes/s)", currentLogFile,
										sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1), throwable);
							}
						}
					};

					StreamProducer<T> producer = StreamProducer.concat(producers);
					resultStage.set(StreamProducerWithResult.of(producer, positionStage));
				})
				.whenException(resultStage::setException);
		return resultStage;
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

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
