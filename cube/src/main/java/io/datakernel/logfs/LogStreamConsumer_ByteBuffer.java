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

import static io.datakernel.stream.StreamProducer.END_OF_STREAM;

import java.util.ArrayDeque;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducers;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LogStreamConsumer_ByteBuffer extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(LogStreamConsumer_ByteBuffer.class);
	private static final long ONE_HOUR = 60 * 60 * 1000L;
	private final String streamId;

	private long currentHour = -1;
	private LogFile currentLogFile;
	private StreamProducers.Idle<ByteBuf> currentProducer;

	private boolean endOfStreamReceived = false;
	private Exception receivedException = null;

	private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();

	private final DateTimeFormatter datetimeFormat;
	private final LogFileSystem fileSystem;

	private int activeWriters = 0;

	public LogStreamConsumer_ByteBuffer(Eventloop eventloop, DateTimeFormatter datetimeFormat, LogFileSystem fileSystem,
	                                    String streamId) {
		super(eventloop);
		this.streamId = streamId;
		this.datetimeFormat = datetimeFormat;
		this.fileSystem = fileSystem;
		this.currentProducer = new StreamProducers.Idle<>(eventloop);
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	private CompletionCallback createCloseCompletionCallback() {
		return new CompletionCallback() {
			@Override
			public void onComplete() {
				--activeWriters;
				if (endOfStreamReceived && activeWriters == 0) {
					closeUpstream();
				} else if (receivedException != null) {
					closeUpstreamWithError(receivedException);
				} else {
					resumeUpstream();
				}
			}

			@Override
			public void onException(Exception exception) {
				--activeWriters;
				closeUpstreamWithError(exception);
			}
		};
	}

	private boolean isWriteStreamAvailable() {
		assert currentProducer != null;
		return currentProducer.getDownstream() != null && currentProducer.getStatus() < END_OF_STREAM;
	}

	@Override
	public void onData(ByteBuf buf) {
		long timestamp = eventloop.currentTimeMillis();
		long newHour = timestamp / ONE_HOUR;
		if (isWriteStreamAvailable()) {
			if (newHour == currentHour) {
				currentProducer.getDownstreamDataReceiver().onData(buf);
				return;
			}
			currentProducer.sendEndOfStream();
		}
		queue.add(buf);
		if (queue.size() > 10) {
			suspendUpstream();
		} else {
			resumeUpstream();
		}

		if (newHour != currentHour) {
			currentHour = newHour;
			String newChunkName = datetimeFormat.print(timestamp);
			if (currentLogFile == null || !newChunkName.equals(currentLogFile.getName())) {
				createWriteStream(newChunkName);
			}
		}
	}

	private void createWriteStream(final String newChunkName) {
		fileSystem.makeUniqueLogFile(streamId, newChunkName, new ResultCallback<LogFile>() {
			@Override
			public void onResult(LogFile result) {
				++activeWriters;

				currentLogFile = result;
				StreamConsumer<ByteBuf> writer = fileSystem.writer(streamId, currentLogFile);
				currentProducer = new StreamProducers.Idle<>(eventloop);
				currentProducer.streamTo(writer);
				writer.addCompletionCallback(createCloseCompletionCallback());

				for (ByteBuf buf : queue) {
					currentProducer.getDownstreamDataReceiver().onData(buf);
				}
				queue.clear();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("{}: creating new unique log file with name {} and stream id {} failed.",
						LogStreamConsumer_ByteBuffer.this, newChunkName, streamId);
				closeUpstreamWithError(exception);
			}
		});
	}

	@Override
	public void onEndOfStream() {
		logger.trace("{}: upstream producer {} endOfStream.", this, upstreamProducer);

		endOfStreamReceived = true;
		if (isWriteStreamAvailable()) {
			currentProducer.sendEndOfStream();
		}
	}

	@Override
	public void onError(Exception e) {
		logger.trace("{}: upstream producer {} error.", this, upstreamProducer, e);

		receivedException = e;
		if (isWriteStreamAvailable()) {
			currentProducer.closeWithError(e);
		}
	}
}
