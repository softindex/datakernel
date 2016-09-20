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

import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.StreamConsumerDecorator;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import org.joda.time.format.DateTimeFormatter;

public class LogStreamConsumer<T> extends StreamConsumerDecorator<T> {
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	public static final int DEFAULT_FLUSH_DELAY = 1000; // 1 second

	private final LogStreamConsumer_ByteBuffer logStreamConsumer_byteBuffer;

	private LogStreamConsumer(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer,
	                          String logPartition, DateTimeFormatter dateTimeFormatter, long fileSwitchPeriod,
	                          int bufferSize, int flushDelayMillis) {
		StreamBinarySerializer<T> streamBinarySerializer = StreamBinarySerializer.create(eventloop, serializer, bufferSize, StreamBinarySerializer.MAX_SIZE, flushDelayMillis, true);
		StreamLZ4Compressor streamCompressor = StreamLZ4Compressor.fastCompressor(eventloop);
		logStreamConsumer_byteBuffer = LogStreamConsumer_ByteBuffer.create(eventloop, dateTimeFormatter, fileSwitchPeriod,
				fileSystem, logPartition);

		logStreamConsumer_byteBuffer.setTag(logPartition);
		streamCompressor.setTag(logPartition);
		streamBinarySerializer.setTag(logPartition);

		setActualConsumer(streamBinarySerializer.getInput());
		streamBinarySerializer.getOutput().streamTo(streamCompressor.getInput());
		streamCompressor.getOutput().streamTo(logStreamConsumer_byteBuffer);
	}

	public static <T> LogStreamConsumer<T> create(Eventloop eventloop, LogFileSystem fileSystem,
	                                              BufferSerializer<T> serializer,
	                                              String logPartition, DateTimeFormatter dateTimeFormatter,
	                                              long fileSwitchPeriod, int bufferSize, int flushDelayMillis) {
		return new LogStreamConsumer<T>(eventloop, fileSystem, serializer, logPartition,
				dateTimeFormatter, fileSwitchPeriod, bufferSize, flushDelayMillis);
	}

	public void setCompletionCallback(CompletionCallback callback) {
		logStreamConsumer_byteBuffer.setCompletionCallback(callback);
	}
}
