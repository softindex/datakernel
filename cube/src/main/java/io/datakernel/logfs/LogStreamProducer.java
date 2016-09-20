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

import io.datakernel.async.AsyncIterator;
import io.datakernel.async.IteratorCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Decompressor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class LogStreamProducer<T> extends StreamProducerDecorator<T> {
	private final String logPartition;
	private final LogPosition startPosition;
	private final LogFile endFile;
	private LogFile currentLogFile;
	private StreamLZ4Decompressor currentDecompressor;
	private LogFileSystem fileSystem;
	private BufferSerializer<T> serializer;
	private final ResultCallback<LogPosition> positionCallback;
	private final StreamForwarder<T> forwarder;
	private final Eventloop eventloop;

	private LogStreamProducer(final Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer,
	                          String logPartition, LogPosition startPosition, LogFile endFile,
	                          ResultCallback<LogPosition> positionCallback) {
		this.eventloop = eventloop;
		this.logPartition = logPartition;
		this.startPosition = startPosition;
		this.endFile = endFile;
		this.fileSystem = fileSystem;
		this.serializer = serializer;
		this.positionCallback = positionCallback;
		this.forwarder = StreamForwarder.create(eventloop);
		setActualProducer(forwarder.getOutput());
		fileSystem.list(logPartition, new ResultCallback<List<LogFile>>() {
			@Override
			public void onResult(List<LogFile> entries) {
				producerForList(entries).streamTo(forwarder.getInput());
			}

			@Override
			public void onException(Exception exception) {
				new StreamProducers.ClosingWithError<T>(eventloop, exception).streamTo(forwarder.getInput());
			}
		});
	}

	public static <T> LogStreamProducer<T> create(final Eventloop eventloop, LogFileSystem fileSystem,
	                                              BufferSerializer<T> serializer, String logPartition,
	                                              LogPosition startPosition, LogFile endFile,
	                                              ResultCallback<LogPosition> positionCallback) {
		return new LogStreamProducer<T>(eventloop, fileSystem, serializer, logPartition,
				startPosition, endFile, positionCallback);
	}

	private boolean readFile(LogFile logFile) {
		if (startPosition.getLogFile() != null && logFile.compareTo(startPosition.getLogFile()) < 0)
			return false;

		if (endFile != null && logFile.compareTo(endFile) > 0)
			return false;

		return true;
	}

	private StreamProducer<T> producerForList(List<LogFile> logFiles) {
		List<LogFile> logFilesToRead = new ArrayList<>(logFiles.size());
		for (LogFile logFile : logFiles) {
			if (readFile(logFile))
				logFilesToRead.add(logFile);
		}
		Collections.sort(logFilesToRead);
		final Iterator<LogFile> it = logFilesToRead.iterator();
		return StreamProducers.concat(eventloop, new AsyncIterator<StreamProducer<T>>() {
			@Override
			public void next(final IteratorCallback<StreamProducer<T>> callback) {
				if (!it.hasNext()) {
					callback.onEnd();
					if (positionCallback != null) {
						positionCallback.onResult(getLogPosition());
					}
					return;
				}

				boolean first = currentLogFile == null;
				currentLogFile = it.next();

				currentDecompressor = StreamLZ4Decompressor.create(eventloop);

				fileSystem.read(logPartition, currentLogFile, first ? startPosition.getPosition() : 0L,
						currentDecompressor.getInput());

				StreamBinaryDeserializer<T> currentDeserializer = StreamBinaryDeserializer.create(eventloop, serializer,
						StreamBinarySerializer.MAX_SIZE);
				ErrorIgnoringTransformer<T> errorIgnoringTransformer = ErrorIgnoringTransformer.create(eventloop);

				currentDecompressor.getOutput().streamTo(currentDeserializer.getInput());
				currentDeserializer.getOutput().streamTo(errorIgnoringTransformer.getInput());

				callback.onNext(errorIgnoringTransformer.getOutput());
			}
		});
	}

	public String getLogPartition() {
		return logPartition;
	}

	public LogPosition getLogPosition() {
		if (currentLogFile == null)
			return startPosition;

		if (currentLogFile.equals(startPosition.getLogFile()))
			return LogPosition.create(currentLogFile, startPosition.getPosition() + currentDecompressor.getInputStreamPosition());

		return LogPosition.create(currentLogFile, currentDecompressor.getInputStreamPosition());
	}
}
