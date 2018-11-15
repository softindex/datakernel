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
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.TruncatedDataException;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.serial.processor.SerialBinaryDeserializer;
import io.datakernel.serial.processor.SerialBinarySerializer;
import io.datakernel.serial.processor.SerialLZ4Compressor;
import io.datakernel.serial.processor.SerialLZ4Decompressor;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierWithResult;
import io.datakernel.util.MemSize;
import io.datakernel.util.Preconditions;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class LogManagerImpl<T> implements LogManager<T>, EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(LogManagerImpl.class);

	public static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH").withZone(ZoneOffset.UTC);
	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(256);

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
	public Promise<StreamConsumer<T>> consumer(String logPartition) {
		validateLogPartition(logPartition);

		return Promise.of(StreamConsumer.<T>ofSupplier(
				supplier -> supplier
						.apply(SerialBinarySerializer.create(serializer)
								.withAutoFlushInterval(autoFlushInterval)
								.withInitialBufferSize(bufferSize)
								.withSkipSerializationErrors())
						.apply(SerialLZ4Compressor.createFastCompressor())
						.bindTo(LogStreamChunker.create(fileSystem, dateTimeFormatter, logPartition)))
				.withLateBinding());
	}

	@Override
	public Promise<StreamSupplierWithResult<T, LogPosition>> supplier(String logPartition,
			LogFile startLogFile, long startOffset,
			LogFile endLogFile) {
		validateLogPartition(logPartition);
		LogPosition startPosition = LogPosition.create(startLogFile, startOffset);
		SettablePromise<LogPosition> positionPromise = new SettablePromise<>();
		SettablePromise<StreamSupplierWithResult<T, LogPosition>> resultPromise = new SettablePromise<>();
		fileSystem.list(logPartition)
				.whenResult(logFiles -> {
					List<LogFile> logFilesToRead = logFiles.stream()
							.filter(logFile -> isFileInRange(logFile, startPosition, endLogFile))
							.sorted()
							.collect(Collectors.toList());

					Iterator<LogFile> it = logFilesToRead.iterator();

					Iterator<StreamSupplier<T>> suppliers = new Iterator<StreamSupplier<T>>() {
						private int n;

						private LogFile currentLogFile;
						private long inputStreamPosition;

						final Stopwatch sw = Stopwatch.createUnstarted();

						@Override
						public boolean hasNext() {
							if (it.hasNext()) return true;
							positionPromise.set(getLogPosition());
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
						public StreamSupplier<T> next() {
							currentLogFile = it.next();
							long position = n++ == 0 ? startPosition.getPosition() : 0L;

							if (logger.isTraceEnabled())
								logger.trace("Read log file `{}` from: {}", currentLogFile, position);

							return StreamSupplier.ofPromise(fileSystem.read(logPartition, currentLogFile, position)
									.thenApply(fileStream -> {
										inputStreamPosition = 0L;
										sw.reset().start();
										return fileStream
												.apply(SerialLZ4Decompressor.create()
														.withInspector((self, header, inputBuf, outputBuf) -> inputStreamPosition += SerialLZ4Decompressor.HEADER_LENGTH + header.compressedLen))
												.apply(supplier ->
														supplier.withEndOfStream(eos ->
																eos.thenComposeEx(($, e) -> (e == null || e instanceof TruncatedDataException) ?
																		Promise.complete() :
																		Promise.ofException(e))))
												.apply(SerialBinaryDeserializer.create(serializer))
												.withEndOfStream(eos ->
														eos.whenComplete(($, e) -> log(e)))
												.withLateBinding();
									}));
						}

						private void log(Throwable e) {
							if (e == null && logger.isTraceEnabled()) {
								logger.trace("Finish log file `{}` in {}, compressed bytes: {} ({} bytes/s)", currentLogFile,
										sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1));
							} else if (e != null && logger.isErrorEnabled()) {
								logger.error("Error on log file `{}` in {}, compressed bytes: {} ({} bytes/s)", currentLogFile,
										sw, inputStreamPosition, inputStreamPosition / Math.max(sw.elapsed(SECONDS), 1), e);
							}
						}
					};

					StreamSupplier<T> supplier = StreamSupplier.concat(suppliers).withLateBinding();
					resultPromise.set(StreamSupplierWithResult.of(supplier, positionPromise));
				})
				.whenException(resultPromise::setException);
		return resultPromise;
	}

	private static void validateLogPartition(String logPartition) {
		Preconditions.checkArgument(!logPartition.contains("-"), "Using dash (-) in log partition name is not allowed");
	}

	private boolean isFileInRange(LogFile logFile, LogPosition startPosition, @Nullable LogFile endFile) {
		if (startPosition.getLogFile() != null && logFile.compareTo(startPosition.getLogFile()) < 0)
			return false;

		return endFile == null || logFile.compareTo(endFile) <= 0;
	}

	public DateTimeFormatter getDateTimeFormatter() {
		return dateTimeFormatter;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}
