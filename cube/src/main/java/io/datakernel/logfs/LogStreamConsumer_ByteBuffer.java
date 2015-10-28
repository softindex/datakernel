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
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.StreamStatus;
import io.datakernel.stream.file.StreamFileWriter;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;

class LogStreamConsumer_ByteBuffer extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(LogStreamConsumer_ByteBuffer.class);
	private static final long ONE_HOUR = 60 * 60 * 1000L;
	private final String streamId;

	private long currentHour = -1;
	private LogFile currentLogFile;
	private StreamProducers.Idle<ByteBuf> currentProducer;
	private StreamFileWriter currentConsumer;

	private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();

	private final DateTimeFormatter datetimeFormat;
	private final LogFileSystem fileSystem;

	private int activeWriters = 0;

	private CompletionCallback callback;

	public LogStreamConsumer_ByteBuffer(Eventloop eventloop, DateTimeFormatter datetimeFormat, LogFileSystem fileSystem,
	                                    String streamId) {
		super(eventloop);
		this.streamId = streamId;
		this.datetimeFormat = datetimeFormat;
		this.fileSystem = fileSystem;
		this.currentProducer = new StreamProducers.Idle<>(eventloop);
	}

	public void setCompletionCallback(CompletionCallback callback) {
		this.callback = callback;
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
				if (getConsumerStatus() == StreamStatus.END_OF_STREAM && activeWriters == 0) {
					if (callback != null)
						callback.onComplete();
				} else if (error != null) {
					if (callback != null)
						callback.onException(error);
				} else {
					resume();
				}
			}

			@Override
			public void onException(Exception exception) {
				--activeWriters;
				if (callback != null)
					callback.onException(exception);
			}
		};
	}

	private boolean isWriteStreamAvailable() {
		return currentConsumer != null && !currentConsumer.getConsumerStatus().isClosed();
	}

	@Override
	public void onData(ByteBuf buf) {
		long timestamp = eventloop.currentTimeMillis();
		long newHour = timestamp / ONE_HOUR;
		if (isWriteStreamAvailable()) {
			if (newHour == currentHour) {
				currentConsumer.getDataReceiver().onData(buf);
				return;
			}
			new StreamProducers.EndOfStream<ByteBuf>(eventloop).streamTo(currentConsumer);
		}
		queue.add(buf);
		if (queue.size() > 10) {
			suspend();
		} else {
			resume();
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
				currentConsumer = fileSystem.writer(streamId, currentLogFile);
				currentProducer = new StreamProducers.Idle<>(eventloop);
				currentProducer.streamTo(currentConsumer);
				currentConsumer.setFlushCallback(createCloseCompletionCallback());

				for (ByteBuf buf : queue) {
					currentConsumer.getDataReceiver().onData(buf);
				}
				queue.clear();

				if (getConsumerStatus() == StreamStatus.END_OF_STREAM) {
					new StreamProducers.EndOfStream<ByteBuf>(eventloop).streamTo(currentConsumer);
				}
			}

			@Override
			public void onException(Exception exception) {
				logger.error("{}: creating new unique log file with name {} and stream id {} failed.",
						LogStreamConsumer_ByteBuffer.this, newChunkName, streamId);
				closeWithError(exception);
			}
		});
	}

	@Override
	public void onEndOfStream() {
		logger.trace("{}: upstream producer {} endOfStream.", this, upstreamProducer);

		if (isWriteStreamAvailable()) {
			new StreamProducers.EndOfStream<ByteBuf>(eventloop).streamTo(currentConsumer);
		}
	}

	@Override
	public void onError(Exception e) {
		logger.trace("{}: upstream producer {} error.", this, upstreamProducer, e);

		if (isWriteStreamAvailable()) {
			new StreamProducers.ClosingWithError<ByteBuf>(eventloop, e).streamTo(currentConsumer);
		}
		cleanupQueue();
	}

	protected void cleanupQueue() {
		for (ByteBuf byteBuf: queue) {
			byteBuf.recycle();
		}
	}


}
