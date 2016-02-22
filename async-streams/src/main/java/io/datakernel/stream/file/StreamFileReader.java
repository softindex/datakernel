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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static java.lang.Math.min;

/**
 * This class allows you to read data from file non-blocking. It represents a {@link AbstractStreamProducer}
 * which streams data from file.
 */
public final class StreamFileReader extends AbstractStreamProducer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(StreamFileReader.class);

	private final AsyncFile asyncFile;

	private final int bufferSize;
	private long position;
	private long length;
	private boolean pendingAsyncOperation;
	private ResultCallback<Long> positionCallback;

	// creators
	private StreamFileReader(Eventloop eventloop, AsyncFile asyncFile,
	                         int bufferSize, long position, long length) {
		super(eventloop);
		this.asyncFile = asyncFile;
		this.bufferSize = bufferSize;
		this.position = position;
		this.length = length;
	}

	public static StreamFileReader readFileSegment(Eventloop eventloop, AsyncFile asyncFile,
	                                               int bufferSize, long position, long length) {
		return new StreamFileReader(eventloop, asyncFile, bufferSize, position, length);
	}

	public static StreamFileReader readFileFrom(final Eventloop eventloop, ExecutorService executor,
	                                            final int bufferSize, Path path,
	                                            final long startPosition) throws IOException {
		AsyncFile asyncFile = getAsyncFile(eventloop, executor, path);
		return readFileFrom(eventloop, asyncFile, bufferSize, startPosition);
	}

	public static StreamFileReader readFileFrom(Eventloop eventloop, AsyncFile asyncFile,
	                                            int bufferSize, long position) {
		return new StreamFileReader(eventloop, asyncFile, bufferSize, position, Long.MAX_VALUE);
	}

	public static StreamFileReader readFileFully(final Eventloop eventloop, ExecutorService executor,
	                                             final int bufferSize, Path path) throws IOException {
		AsyncFile asyncFile = getAsyncFile(eventloop, executor, path);
		return readFileFully(eventloop, asyncFile, bufferSize);
	}

	public static StreamFileReader readFileFully(Eventloop eventloop, AsyncFile asyncFile, int bufferSize) {
		return new StreamFileReader(eventloop, asyncFile, bufferSize, 0, Long.MAX_VALUE);
	}

	// api
	public void setPositionCallback(ResultCallback<Long> positionCallback) {
		if (getProducerStatus().isOpen()) {
			this.positionCallback = positionCallback;
		} else {
			if (getProducerStatus() == StreamStatus.END_OF_STREAM) {
				positionCallback.onResult(position);
			} else {
				positionCallback.onException(getProducerException());
			}
		}
	}

	public long getPosition() {
		return position;
	}

	// functional
	protected void doFlush() {
		if (getProducerStatus().isClosed()) {
			return;
		}

		if (length == 0L) {
			doCleanup(ignoreCompletionCallback());
			sendEndOfStream();
			return;
		}

		final ByteBuf buf = ByteBufPool.allocate((int) min(bufferSize, length));

		asyncFile.read(buf, position, new ResultCallback<Integer>() {
			@Override
			public void onResult(Integer result) {
				if (getProducerStatus().isClosed()) {
					buf.recycle();
					doCleanup(ignoreCompletionCallback());
					return;
				}
				pendingAsyncOperation = false;
				if (result == -1) {
					buf.recycle();
					doCleanup(ignoreCompletionCallback());
					sendEndOfStream();

					if (positionCallback != null) {
						positionCallback.onResult(position);
					}

					return;
				} else {
					position += result;
					buf.flip();
					send(buf);
					if (length != Long.MAX_VALUE) {
						length -= result;
					}
				}
				if (isStatusReady()) {
					postFlush();
				}
			}

			@Override
			public void onException(Exception e) {
				buf.recycle();
				doCleanup(ignoreCompletionCallback());
				closeWithError(e);

				if (positionCallback != null) {
					positionCallback.onException(e);
				}
			}
		});
	}

	protected void postFlush() {
		if (!pendingAsyncOperation) {
			pendingAsyncOperation = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					doFlush();
				}
			});
			logger.trace("{}: posted flush", this);
		}
	}

	@Override
	public void onSuspended() {
		logger.trace("{}: downstream consumer {} suspended.", this, downstreamConsumer);
	}

	@Override
	public void onResumed() {
		logger.trace("{}: downstream consumer {} resumed.", this, downstreamConsumer);
		postFlush();
	}

	@Override
	protected void onStarted() {
		logger.info("{}: started reading", this);
		postFlush();
	}

	@Override
	protected void onDataReceiverChanged() {
		//  empty
	}

	@Override
	protected void onError(Exception e) {
		logger.error("{}: onError", this, e);
	}

	protected void doCleanup(final CompletionCallback callback) {
		logger.info("{}: finished reading", this);
		asyncFile.close(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.trace("{}: closed file", this);
				callback.onComplete();
			}

			@Override
			public void onException(Exception exception) {
				logger.error("{}: failed to close file", this, exception);
				callback.onException(exception);
			}
		});
	}

	private static AsyncFile getAsyncFile(Eventloop eventloop, ExecutorService executor, Path path) throws IOException {
		return AsyncFile.open(eventloop, executor, path, new OpenOption[]{StandardOpenOption.READ});
	}

	@Override
	public String toString() {
		return "StreamFileReader{" + asyncFile +
				", pos=" + position +
				(length == Long.MAX_VALUE ? "" : ", len=" + length) +
				(pendingAsyncOperation ? ", pendingAsyncOperation" : "") +
				'}';
	}
}

