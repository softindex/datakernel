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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

class LogStreamConsumer_ByteBuffer extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(LogStreamConsumer_ByteBuffer.class);
	private final String streamId;

	private long currentHour = -1;
	private LogFile currentLogFile;
	private StreamProducers.Idle<ByteBuf> currentProducer;
	private final StreamConsumerSwitcher<ByteBuf> currentProducerSwitcher = new StreamConsumerSwitcher<>(eventloop);
	private long currentFileSize = 0L;

	private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();

	private final DateTimeFormatter datetimeFormat;
	private final LogFileSystem fileSystem;

	public LogStreamConsumer_ByteBuffer(Eventloop eventloop, DateTimeFormatter datetimeFormat, LogFileSystem fileSystem,
	                                    String streamId) {
		super(eventloop);
		this.streamId = streamId;
		this.datetimeFormat = datetimeFormat;
		this.fileSystem = fileSystem;
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	@Override
	public void onData(ByteBuf buf) {
		if (currentProducer == null) {
			queue.add(buf);
			if (queue.size() > 10) {
				suspendUpstream();
			}
		} else {
			currentFileSize += buf.remaining();
			currentProducer.getDownstreamDataReceiver().onData(buf);
		}

		long timestamp = eventloop.currentTimeMillis();
		long newHour = timestamp / (1000 * 60 * 30);
		if (newHour != currentHour) {
			currentHour = newHour;
			final String newChunkName = datetimeFormat.print(timestamp);
			if (currentLogFile == null || !newChunkName.equals(currentLogFile.getName())) {
				fileSystem.makeUniqueLogFile(streamId, newChunkName, new ResultCallback<LogFile>() {
					@Override
					public void onResult(LogFile result) {
						currentLogFile = result;
						currentFileSize = 0L;

						if (currentProducer == null) {
							currentProducer = new StreamProducers.Idle<>(eventloop);
							currentProducer.streamTo(currentProducerSwitcher);
						}

						StreamConsumer<ByteBuf> writer = fileSystem.writer(streamId, currentLogFile);

						currentProducerSwitcher.switchConsumerTo(writer);

						for (ByteBuf buf : queue) {
							currentFileSize += buf.remaining();
							currentProducer.getDownstreamDataReceiver().onData(buf);
						}

						queue.clear();
					}

					@Override
					public void onException(Exception exception) {
						logger.error("{}: creating new unique log file with name {} and stream id {} failed.",
								LogStreamConsumer_ByteBuffer.this, newChunkName, streamId);
					}
				});
			}
		}
	}

	@Override
	public void onProducerEndOfStream() {
		logger.trace("{}: current producer {} endOfStream.", this, currentProducer);
		if (currentProducer != null) {
			currentProducer.sendEndOfStream();
		}
	}

	@Override
	public void onProducerError(Exception e) {
		if (currentProducer != null) {
			currentProducer.onConsumerError(e);
		}
	}
}
