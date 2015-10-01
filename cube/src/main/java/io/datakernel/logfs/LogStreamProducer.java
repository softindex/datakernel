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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.*;
import io.datakernel.stream.file.StreamFileReader;
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
	private LogFile currentLogFile;
	private StreamLZ4Decompressor currentDecompressor;
	private StreamFileReader currentReader;
	private LogFileSystem fileSystem;
	private BufferSerializer<T> serializer;
	private final ResultCallback<LogPosition> positionCallback;
	private final StreamForwarder<T> forwarder;

	public LogStreamProducer(final Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer,
	                         String logPartition, LogPosition startPosition, ResultCallback<LogPosition> positionCallback) {
		super(eventloop);
		this.logPartition = logPartition;
		this.startPosition = startPosition;
		this.fileSystem = fileSystem;
		this.serializer = serializer;
		this.positionCallback = positionCallback;
		this.forwarder = new StreamForwarder<>(eventloop);
		decorate(forwarder);
		fileSystem.list(logPartition, new ResultCallback<List<LogFile>>() {
			@Override
			public void onResult(List<LogFile> entries) {
				producerForList(entries).streamTo(forwarder);
			}

			@Override
			public void onException(Exception exception) {
				new StreamProducers.ClosingWithError<T>(eventloop, exception).streamTo(forwarder);
			}
		});
	}

	private StreamProducer<T> producerForList(List<LogFile> logFiles) {
		List<LogFile> logFilesToRead;
		if (startPosition.getLogFile() != null) {
			logFilesToRead = new ArrayList<>(logFiles.size());
			for (LogFile logFile : logFiles) {
				if (logFile.compareTo(startPosition.getLogFile()) >= 0) {
					logFilesToRead.add(logFile);
				}
			}
		} else {
			logFilesToRead = logFiles;
		}
		Collections.sort(logFilesToRead);
		final Iterator<LogFile> it = logFilesToRead.iterator();
		return StreamProducers.concat(eventloop, new AsyncIterator<StreamProducer<T>>() {
			@Override
			public void next(IteratorCallback<StreamProducer<T>> callback) {
				if (!it.hasNext()) {
					callback.onEnd();
					if (positionCallback != null) {
						positionCallback.onResult(getLogPosition());
					}
					return;
				}

				boolean first = currentLogFile == null;
				currentLogFile = it.next();

				currentReader = fileSystem.reader(logPartition, currentLogFile, first ? startPosition.getPosition() : 0L);
				currentDecompressor = new StreamLZ4Decompressor(eventloop);
				StreamBinaryDeserializer<T> currentDeserializer = new StreamBinaryDeserializer<>(eventloop, serializer, StreamBinarySerializer.MAX_SIZE);
				ErrorIgnoringTransformer<T> errorIgnoringTransformer = new ErrorIgnoringTransformer<>(eventloop);

				currentReader.streamTo(currentDecompressor);
				currentDecompressor.streamTo(currentDeserializer);
				currentDeserializer.streamTo(errorIgnoringTransformer);

				callback.onNext(errorIgnoringTransformer);
			}
		});
	}

	public String getLogPartition() {
		return logPartition;
	}

	public LogPosition getLogPosition() {
		return currentLogFile == null ? startPosition : new LogPosition(currentLogFile, currentReader.getPosition());
	}

}
