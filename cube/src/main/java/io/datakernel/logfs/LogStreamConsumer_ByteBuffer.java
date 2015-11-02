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
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamConsumerDecorator;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamStatus;
import io.datakernel.stream.file.StreamFileWriter;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogStreamConsumer_ByteBuffer extends StreamConsumerDecorator<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(LogStreamConsumer_ByteBuffer.class);
	private final StreamWriteLog streamWriteLog;

	public LogStreamConsumer_ByteBuffer(Eventloop eventloop, DateTimeFormatter datetimeFormat, LogFileSystem fileSystem, String streamId) {
		this.streamWriteLog = new StreamWriteLog(eventloop, datetimeFormat, fileSystem, streamId);
		setActualConsumer(streamWriteLog.getInput());
	}

	public void setCompletionCallback(CompletionCallback completionCallback) {
		streamWriteLog.upstreamConsumer.callback = completionCallback;
	}

	public void setTag(Object tag) {
		streamWriteLog.setTag(tag);
	}

	private class StreamWriteLog extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> {
		private UpstreamConsumer upstreamConsumer;
		private DownstreamProducer downstreamProducer;

		protected StreamWriteLog(Eventloop eventloop, DateTimeFormatter datetimeFormat, LogFileSystem fileSystem, String streamId) {
			super(eventloop);
			downstreamProducer = new DownstreamProducer();
			upstreamConsumer = new UpstreamConsumer(datetimeFormat, fileSystem, streamId);
		}

		private class UpstreamConsumer extends AbstractUpstreamConsumer implements StreamDataReceiver<ByteBuf> {
			private static final long ONE_HOUR = 60 * 60 * 1000L;
			private final String streamId;

			private long currentHour = -1;
			private LogFile currentLogFile;

			private final DateTimeFormatter datetimeFormat;
			private final LogFileSystem fileSystem;
			private boolean createFile;
			private boolean newFile;

			private int activeWriters = 0;

			private CompletionCallback callback;

			public UpstreamConsumer(DateTimeFormatter datetimeFormat, LogFileSystem fileSystem, String streamId) {
				this.streamId = streamId;
				this.datetimeFormat = datetimeFormat;
				this.fileSystem = fileSystem;
			}

			@Override
			protected void onUpstreamEndOfStream() {
				logger.trace("{}: upstream producer {} endOfStream.", this, upstreamProducer);

				if (downstreamProducer.getDownstream() != null) {
					downstreamProducer.sendEndOfStream();
				}

				if (activeWriters == 0) {
					zeroActiveWriters();
				}
			}

			@Override
			public StreamDataReceiver<ByteBuf> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(ByteBuf buf) {
				long timestamp = eventloop.currentTimeMillis();
				long newHour = timestamp / ONE_HOUR;
				downstreamProducer.send(buf);

				if (newHour != currentHour && createFile) newFile = true;
				if (newHour != currentHour && !createFile) {
					currentHour = newHour;
					String chunkName = datetimeFormat.print(timestamp);
					if (currentLogFile == null || !chunkName.equals(currentLogFile.getName())) {
						createWriteStream(chunkName);
					}
				}
			}

			private void createWriteStream(final String newChunkName) {
				createFile = true;
				fileSystem.makeUniqueLogFile(streamId, newChunkName, new ResultCallback<LogFile>() {
					@Override
					public void onResult(LogFile result) {
						createFile = false;
						++activeWriters;

						if (downstreamProducer.getDownstream() != null) {
							downstreamProducer.getDownstream().onProducerEndOfStream();
						}

						currentLogFile = result;
						StreamFileWriter currentConsumer = fileSystem.writer(streamId, currentLogFile);
						ConsumerErrorIgnoring consumerErrorIgnoring = new ConsumerErrorIgnoring(eventloop);
						downstreamProducer.streamTo(consumerErrorIgnoring.getInput());
						consumerErrorIgnoring.getOutput().streamTo(currentConsumer);
						currentConsumer.setFlushCallback(createCloseCompletionCallback());

						if (getConsumerStatus() == StreamStatus.END_OF_STREAM) {
							currentConsumer.onProducerEndOfStream();
						}
						if (getConsumerStatus() == StreamStatus.CLOSED_WITH_ERROR) {
							currentConsumer.onProducerError(getConsumerException());
						}
						if (newFile) {
							newFile = false;
							checkHour();
						}
					}

					@Override
					public void onException(Exception exception) {
						createFile = false;
						logger.error("{}: creating new unique log file with name {} and stream id {} failed.",
								LogStreamConsumer_ByteBuffer.this, newChunkName, streamId);

						eventloop.schedule(1000L, new Runnable() {
							@Override
							public void run() {
								createWriteStream(newChunkName);
							}
						});
					}
				});
			}

			private void checkHour() {
				long timestamp = eventloop.currentTimeMillis();

				currentHour = timestamp / ONE_HOUR;
				String chunkName = datetimeFormat.print(timestamp);
				if (currentLogFile == null || !chunkName.equals(currentLogFile.getName())) {
					createWriteStream(chunkName);
				}
			}

			private void zeroActiveWriters() {
				if (getConsumerStatus() == StreamStatus.END_OF_STREAM) {
					if (callback != null) {
						callback.onComplete();
						callback = null;
					}
				} else if (error != null) {
					if (callback != null) {
						callback.onException(getConsumerException());
						callback = null;
					}
				} else {
					resume();
				}
			}

			private CompletionCallback createCloseCompletionCallback() {
				return new SimpleCompletionCallback() {
					@Override
					protected void onCompleteOrException() {
						--activeWriters;
						if (activeWriters == 0) {
							zeroActiveWriters();
						} else {
							resume();
						}
					}
				};
			}
		}

		private class DownstreamProducer extends AbstractDownstreamProducer {
			@Override
			protected void doCleanup() {
				for (ByteBuf byteBuf : downstreamProducer.bufferedList) {
					byteBuf.recycle();
				}
				downstreamProducer.bufferedList.clear();
			}

			@Override
			protected void onDownstreamSuspended() {
				upstreamConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				upstreamConsumer.resume();
			}
		}
	}

	private class ConsumerErrorIgnoring extends AbstractStreamTransformer_1_1<ByteBuf, ByteBuf> {
		private UpstreamConsumer upstreamConsumer;
		private DownstreamProducer downstreamProducer;

		protected ConsumerErrorIgnoring(Eventloop eventloop) {
			super(eventloop);
			upstreamConsumer = new UpstreamConsumer();
			downstreamProducer = new DownstreamProducer();
		}

		private class UpstreamConsumer extends AbstractUpstreamConsumer {

			@Override
			protected void onUpstreamEndOfStream() {
				downstreamProducer.sendEndOfStream();
			}

			@Override
			public StreamDataReceiver<ByteBuf> getDataReceiver() {
				return downstreamProducer.getDownstreamDataReceiver();
			}
		}

		private class DownstreamProducer extends AbstractDownstreamProducer {

			@Override
			protected void onDownstreamSuspended() {
				upstreamConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				upstreamConsumer.resume();
			}

			@Override
			protected void onError(Exception e) {
				// do nothing
			}
		}

	}
}