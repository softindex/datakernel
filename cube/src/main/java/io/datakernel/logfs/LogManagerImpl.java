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
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public final class LogManagerImpl<T> implements LogManager<T> {
	public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH").withZone(DateTimeZone.UTC);
	public static final int DEFAULT_BUFFER_SIZE = LogStreamConsumer.DEFAULT_BUFFER_SIZE;
	public static final int DEFAULT_FLUSH_DELAY = LogStreamConsumer.DEFAULT_FLUSH_DELAY;

	private final Eventloop eventloop;
	private final LogFileSystem fileSystem;
	private final BufferSerializer<T> serializer;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private int flushDelayMillis = DEFAULT_FLUSH_DELAY;

	public LogManagerImpl(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer) {
		this.eventloop = eventloop;
		this.fileSystem = fileSystem;
		this.serializer = serializer;
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
		return consumer(logPartition, ignoreCompletionCallback());
	}

	@Override
	public LogStreamConsumer<T> consumer(String logPartition, CompletionCallback callback) {
		LogStreamConsumer<T> logStreamConsumer = new LogStreamConsumer<>(eventloop, fileSystem, serializer, logPartition, bufferSize, flushDelayMillis);
		logStreamConsumer.setCompletionCallback(callback);
		return logStreamConsumer;
	}

	@Override
	public LogStreamProducer<T> producer(String logPartition, LogFile startLogFile, long startPosition,
	                                     ResultCallback<LogPosition> positionCallback) {
		return new LogStreamProducer<>(eventloop, fileSystem, serializer, logPartition,
				new LogPosition(startLogFile, startPosition), null, positionCallback);
	}

	@Override
	public LogStreamProducer<T> producer(String logPartition, LogFile startLogFile, long startPosition,
	                                     LogFile endLogFile, ResultCallback<LogPosition> positionCallback) {
		return new LogStreamProducer<>(eventloop, fileSystem, serializer, logPartition,
				new LogPosition(startLogFile, startPosition), endLogFile,
				positionCallback);
	}

	@Override
	public LogStreamProducer<T> producer(String logPartition, long startTimestamp, long endTimestamp) {
		return producer(logPartition, new LogFile(DATE_TIME_FORMATTER.print(startTimestamp), 0), 0,
				new LogFile(DATE_TIME_FORMATTER.print(endTimestamp), 0),
				AsyncCallbacks.<LogPosition>ignoreResultCallback());
	}
}
