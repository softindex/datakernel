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
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamLZ4Compressor;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LogStreamConsumer<T> extends StreamConsumerDecorator<T> {
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH").withZone(DateTimeZone.UTC);
	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
	public static final int DEFAULT_FLUSH_DELAY = 1000; // 1 second

	private final StreamBinarySerializer<T> streamBinarySerializer;
	private final LogStreamConsumer_ByteBuffer logStreamConsumer_byteBuffer;

	public LogStreamConsumer(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer, String streamId) {
		this(eventloop, fileSystem, serializer, streamId, DEFAULT_BUFFER_SIZE);
	}

	public LogStreamConsumer(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer,
	                         String streamId, int bufferSize) {
		this(eventloop, fileSystem, serializer, streamId, bufferSize, DEFAULT_FLUSH_DELAY);
	}

	public LogStreamConsumer(Eventloop eventloop, LogFileSystem fileSystem, BufferSerializer<T> serializer,
	                         String streamId, int bufferSize, int flushDelayMillis) {
		super(eventloop);
		this.streamBinarySerializer = new StreamBinarySerializer<>(eventloop, serializer, bufferSize, StreamBinarySerializer.MAX_SIZE, flushDelayMillis, false);
		StreamLZ4Compressor streamCompressor = StreamLZ4Compressor.fastCompressor(eventloop);
		logStreamConsumer_byteBuffer = new LogStreamConsumer_ByteBuffer(eventloop, DATE_TIME_FORMATTER, fileSystem, streamId);

		logStreamConsumer_byteBuffer.setTag(streamId);
		streamCompressor.setTag(streamId);
		this.streamBinarySerializer.setTag(streamId);

		setActualConsumer(streamBinarySerializer);
		streamBinarySerializer.streamTo(streamCompressor);
		streamCompressor.streamTo(logStreamConsumer_byteBuffer);
	}

	public void setCompletionCallback(CompletionCallback callback) {
		logStreamConsumer_byteBuffer.setCompletionCallback(callback);
	}
}
