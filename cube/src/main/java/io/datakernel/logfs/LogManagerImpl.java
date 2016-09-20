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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.util.Preconditions;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public final class LogManagerImpl<T> implements LogManager<T> {
	public static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH").withZone(DateTimeZone.UTC);
	public static final DateTimeFormatter DETAILED_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss").withZone(DateTimeZone.UTC);
	public static final long DEFAULT_FILE_SWITCH_PERIOD = 60 * 60 * 1000L; // 1 hour
	public static final int DEFAULT_BUFFER_SIZE = LogStreamConsumer.DEFAULT_BUFFER_SIZE;
	public static final int DEFAULT_FLUSH_DELAY = LogStreamConsumer.DEFAULT_FLUSH_DELAY;

	private final Eventloop eventloop;
	private final LogFileSystem fileSystem;
	private final BufferSerializer<T> serializer;
	private final DateTimeFormatter dateTimeFormatter;
	private final long fileSwitchPeriod;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private int flushDelayMillis = DEFAULT_FLUSH_DELAY;

	private LogManagerImpl(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer) {
		this(eventloop, fileSystem, serializer, DEFAULT_DATE_TIME_FORMATTER, DEFAULT_FILE_SWITCH_PERIOD);
	}

	private LogManagerImpl(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer,
	                       DateTimeFormatter dateTimeFormatter, long fileSwitchPeriod) {
		this.eventloop = eventloop;
		this.fileSystem = fileSystem;
		this.serializer = serializer;
		this.dateTimeFormatter = dateTimeFormatter;
		this.fileSwitchPeriod = fileSwitchPeriod;
	}

	public static <T> LogManagerImpl<T> create(Eventloop eventloop, LogFileSystem fileSystem,
	                                           BufferSerializer<T> serializer) {
		return new LogManagerImpl<T>(eventloop, fileSystem, serializer);
	}

	public static <T> LogManagerImpl<T> create(Eventloop eventloop, LogFileSystem fileSystem,
	                                           BufferSerializer<T> serializer, DateTimeFormatter dateTimeFormatter,
	                                           long fileSwitchPeriod) {
		return new LogManagerImpl<T>(eventloop, fileSystem, serializer, dateTimeFormatter, fileSwitchPeriod);
	}

	public LogManagerImpl<T> fileSystemBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public LogManagerImpl<T> autoFlushDelayMillis(int flushDelayMillis) {
		this.flushDelayMillis = flushDelayMillis;
		return this;
	}

	@Override
	public LogStreamConsumer<T> consumer(String logPartition) {
		validateLogPartition(logPartition);
		return consumer(logPartition, ignoreCompletionCallback());
	}

	@Override
	public LogStreamConsumer<T> consumer(String logPartition, CompletionCallback callback) {
		validateLogPartition(logPartition);
		LogStreamConsumer<T> logStreamConsumer = LogStreamConsumer.create(eventloop, fileSystem, serializer,
				logPartition, dateTimeFormatter, fileSwitchPeriod, bufferSize, flushDelayMillis);
		logStreamConsumer.setCompletionCallback(callback);
		return logStreamConsumer;
	}

	@Override
	public LogStreamProducer<T> producer(String logPartition, LogFile startLogFile, long startPosition,
	                                     ResultCallback<LogPosition> positionCallback) {
		validateLogPartition(logPartition);
		return LogStreamProducer.create(eventloop, fileSystem, serializer, logPartition,
				LogPosition.create(startLogFile, startPosition), null, positionCallback);
	}

	@Override
	public LogStreamProducer<T> producer(String logPartition, LogFile startLogFile, long startPosition,
	                                     LogFile endLogFile, ResultCallback<LogPosition> positionCallback) {
		validateLogPartition(logPartition);
		return LogStreamProducer.create(eventloop, fileSystem, serializer, logPartition,
				LogPosition.create(startLogFile, startPosition), endLogFile,
				positionCallback);
	}

	@Override
	public LogStreamProducer<T> producer(String logPartition, long startTimestamp, long endTimestamp) {
		validateLogPartition(logPartition);
		return producer(logPartition, new LogFile(dateTimeFormatter.print(startTimestamp), 0), 0,
				new LogFile(dateTimeFormatter.print(endTimestamp), 0),
				AsyncCallbacks.<LogPosition>ignoreResultCallback());
	}

	@Override
	public LogStreamProducer<T> producer(String logPartition, String startLogFileName, String endLogFileName) {
		validateLogPartition(logPartition);
		return producer(logPartition, new LogFile(startLogFileName, 0), 0, new LogFile(endLogFileName, 0),
				AsyncCallbacks.<LogPosition>ignoreResultCallback());
	}

	private static void validateLogPartition(String logPartition) {
		Preconditions.checkArgument(!logPartition.contains("-"), "Using dash (-) in log partition name is not allowed");
	}

	public DateTimeFormatter getDateTimeFormatter() {
		return dateTimeFormatter;
	}
}
