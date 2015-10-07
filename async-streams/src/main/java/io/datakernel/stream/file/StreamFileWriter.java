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

package io.datakernel.stream.file;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardOpenOption.*;

/**
 * This class allows you to write data from file non-blocking. It represents consumer which receives
 * data and writes it to file.
 */
public final class StreamFileWriter extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(StreamFileWriter.class);

	private final ExecutorService executor;
	private final Path path;
	private final OpenOption[] options;
	private final boolean removeFileOnException;

	private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();
	private AsyncFile asyncFile;

	private long position;

	private boolean pendingAsyncOperation;

	private static int i = 0;

	/**
	 * Creates a new instance of StreamFileWriter
	 *
	 * @param eventloop event loop in which it will work
	 * @param executor  executor it which file will be opened
	 * @param path      location of file
	 * @param options   options for opening file, it can be {WRITE, APPEND} for appending new data to
	 *                  existing file, {WRITE, CREATE} for creating new file and writing data there
	 */
	public StreamFileWriter(Eventloop eventloop, ExecutorService executor,
	                        Path path, OpenOption[] options) {
		this(eventloop, executor, path, options, false);
	}

	public StreamFileWriter(Eventloop eventloop, ExecutorService executor,
	                        Path path, OpenOption[] options, boolean removeFileOnException) {
		super(eventloop);
		this.executor = checkNotNull(executor);
		this.path = path;
		this.options = options;
		this.removeFileOnException = removeFileOnException;
	}

	/**
	 * Returns new StreamFileWriter for appending new data to existing file
	 *
	 * @param eventloop event loop in which it will work
	 * @param executor  executor in which file will be opened
	 * @param path      location of file
	 */
	public static StreamFileWriter appendFile(Eventloop eventloop, ExecutorService executor,
	                                          Path path) {
		return new StreamFileWriter(eventloop, executor, path, new OpenOption[]{WRITE});
	}

	/**
	 * Returns new StreamFileWriter for creating new file and writing data there
	 *
	 * @param eventloop event loop in which it will work
	 * @param executor  executor it which file will be opened
	 * @param path      location of file
	 */
	public static StreamFileWriter createFile(Eventloop eventloop, ExecutorService executor,
	                                          Path path) {
		return new StreamFileWriter(eventloop, executor, path, new OpenOption[]{WRITE, CREATE, TRUNCATE_EXISTING});
	}

	public static StreamFileWriter createFile(Eventloop eventloop, ExecutorService executor,
	                                          Path path, boolean removeFileOnException) {
		return new StreamFileWriter(eventloop, executor, path, new OpenOption[]{WRITE, CREATE, TRUNCATE_EXISTING}, removeFileOnException);
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}

	private void doFlush() {

		final ByteBuf buf = queue.poll();
		final int len = buf.remaining();

		logger.info("In doFlush: {}, {}, {}, {}", buf, pendingAsyncOperation, queue.size(), getStatus());

		asyncFile.writeFully(buf, position, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Completed writing in file: {}", buf);

				buf.recycle();
				pendingAsyncOperation = false;
				position += len;
				if (queue.size() <= 1) {
					resume();
				}
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						postFlush();
					}
				});
			}

			@Override
			public void onException(final Exception e) {
				logger.info("Failed to write in file: {}", buf);

				pendingAsyncOperation = false;
				logger.error("Can't write data in file", e);
				buf.recycle();
				closeWithError(e);
			}
		});
	}

	private void postFlush() {
		logger.info("In PostFlash: {}, {}, {}", pendingAsyncOperation, queue.size(), getStatus());
		if (getStatus() == END_OF_STREAM && queue.isEmpty() && !pendingAsyncOperation) {
			logger.info("Finishing success");
			doCleanup(new CompletionCallback() {
				@Override
				public void onComplete() {
					close();
				}

				@Override
				public void onException(Exception exception) {
					closeWithError(new Exception("Can't do cleanup for file\t" + path.getFileName()));
				}
			});
		}
		if (!queue.isEmpty() && !pendingAsyncOperation && asyncFile != null) {
			logger.info("writing in file");
			pendingAsyncOperation = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					doFlush();
				}
			});
		}

	}

	@Override
	protected void onStarted() {
		logger.info("StreamFileWriter  onStarted");
		if (asyncFile != null || pendingAsyncOperation)
			return;
		pendingAsyncOperation = true;
		AsyncFile.open(eventloop, executor, path, options, new ResultCallback<AsyncFile>() {
			@Override
			public void onResult(AsyncFile result) {
				logger.info("File {} is opened for writing!", path.getFileName());
				pendingAsyncOperation = false;
				asyncFile = result;
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						postFlush();
					}
				});
			}

			@Override
			public void onException(final Exception e) {
				logger.error("Can't open file {} for writing", path.getFileName(), e);
				pendingAsyncOperation = false;
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						closeWithError(e);
					}
				});
			}
		});
	}

	@Override
	public void onData(ByteBuf buf) {
		checkState(getStatus() < END_OF_STREAM, "Unexpected buf after end-of-stream %s : %s", this, buf);
		logger.info("Receiving data item {}", buf);
		queue.offer(buf);
		if (queue.size() > 1) {
			suspend();
		}
		postFlush();
	}

	private void doCleanup(CompletionCallback callback) {
		if (asyncFile != null) {
			asyncFile.close(callback);
			asyncFile = null;
		}
	}

	@Override
	protected void onEndOfStream() {
		logger.info("endOfStream for {}, upstream: {}", this, upstreamProducer);
		postFlush();
	}

	@Override
	protected void onError(final Exception e) {
		if (pendingAsyncOperation) {
			while (i++ < 10) {
				logger.info("Closing with error! Still there is pendingAsyncOperation");
			}

			eventloop.post(new Runnable() {
				@Override
				public void run() {
					onError(e);
				}
			});
		} else {
			logger.info("Closing with error!");
			doCleanup(new CompletionCallback() {
				private void tryRemoveFile() {
					if (removeFileOnException) {
						try {
							Files.delete(path);
						} catch (IOException e1) {
							logger.error("Could not delete file {}", path.toAbsolutePath(), e1);
						}
					}
					closeWithError(e);
				}

				@Override
				public void onComplete() {
					tryRemoveFile();
				}

				@Override
				public void onException(Exception ignored) {
					tryRemoveFile();
				}
			});
		}
	}
}
