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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.nio.file.StandardOpenOption.*;

/**
 * This class allows you to write data from file non-blocking. It represents consumer which receives
 * data and writes it to file.
 */
public final class StreamFileWriter extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	public static final OpenOption[] CREATE_OPTIONS = new OpenOption[]{WRITE, CREATE, TRUNCATE_EXISTING};

	private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();
	private final AsyncFile asyncFile;

	private long position;

	private boolean forceOnClose;

	private boolean pendingAsyncOperation;

	private SettableStage<Void> flushStage;

	// region creators
	private StreamFileWriter(Eventloop eventloop, AsyncFile asyncFile, boolean forceOnClose) {
		super(eventloop);
		this.asyncFile = asyncFile;
		this.forceOnClose = forceOnClose;
	}

	public static StreamFileWriter create(final Eventloop eventloop, ExecutorService executor, Path path) throws IOException {
		return create(eventloop, executor, path, false);
	}

	public static StreamFileWriter create(final Eventloop eventloop, ExecutorService executor, Path path, boolean forceOnClose) throws IOException {
		AsyncFile asyncFile = AsyncFile.open(eventloop, executor, path, CREATE_OPTIONS);
		return create(eventloop, asyncFile, forceOnClose);
	}

	public static StreamFileWriter create(Eventloop eventloop, AsyncFile asyncFile) {
		return create(eventloop, asyncFile, false);
	}

	public static StreamFileWriter create(Eventloop eventloop, AsyncFile asyncFile, boolean forceOnClose) {
		return new StreamFileWriter(eventloop, asyncFile, forceOnClose);
	}
	// endregion

	// region api
	public CompletionStage<Void> getFlushStage() {
		if (queue.isEmpty() && !pendingAsyncOperation) {
			if (getConsumerStatus() == END_OF_STREAM) {
				return SettableStage.immediateStage(null);
			}

			if (getConsumerStatus() == CLOSED_WITH_ERROR) {
				return SettableStage.immediateFailedStage(getConsumerException());
			}
		}

		return this.flushStage = SettableStage.create();
	}

	@Override
	public StreamDataReceiver<ByteBuf> getDataReceiver() {
		return this;
	}
	// endregion

	// region functional
	private void doFlush() {
		if (getConsumerStatus() == CLOSED_WITH_ERROR)
			return;

		final ByteBuf buf = queue.poll();
		final int length = buf.readRemaining();
		asyncFile.writeFully(buf, position).whenComplete((aVoid, throwable) -> {
			if (throwable != null) {
				logger.error("{}: failed to flush", StreamFileWriter.this, throwable);
				doWriterCleanup(false).whenComplete((aVoid1, throwable1) -> {
					pendingAsyncOperation = false;
					closeWithError(AsyncCallbacks.throwableToException(throwable1));
				});
			} else {
				logger.trace("{}: completed flush", StreamFileWriter.this);
				position += length;
				pendingAsyncOperation = false;
				if (queue.size() <= 1) {
					resume();
				}
				postFlush();
			}
		});
	}

	private void postFlush() {
		if (!queue.isEmpty() && !pendingAsyncOperation) {
			pendingAsyncOperation = true;
			eventloop.post(this::doFlush);
			logger.trace("{}: posted flush", this);
		}
		if (getConsumerStatus() == END_OF_STREAM && queue.isEmpty() && !pendingAsyncOperation) {
			pendingAsyncOperation = true;
			doWriterCleanup(forceOnClose).whenComplete(($, throwable) -> {
				if (throwable == null) {
					pendingAsyncOperation = false;
					logger.info("{}: finished writing", StreamFileWriter.this);
					if (flushStage != null) flushStage.setResult(null);
				} else {
					pendingAsyncOperation = false;
					closeWithError(new Exception("Can't do cleanup for file\t" + asyncFile));

				}
			});
		}
	}

	private CompletionStage<Void> doWriterCleanup(boolean forceOnClose) {
		for (ByteBuf buf : queue) {
			buf.recycle();
		}
		queue.clear();

		return forceOnClose ? asyncFile.forceAndClose() : asyncFile.close();
	}

	@Override
	protected void onStarted() {
		logger.info("{}: started writing", this);
	}

	@Override
	public void onData(ByteBuf buf) {
		queue.offer(buf);
		if (queue.size() > 1) {
			suspend();
		}
		postFlush();
	}

	@Override
	protected void onEndOfStream() {
		logger.trace("endOfStream for {}, upstream: {}", this, getUpstream());
		postFlush();
	}

	@Override
	protected void onError(final Exception e) {
		logger.error("{}: onError", this, e);
		pendingAsyncOperation = true;

		doWriterCleanup(false).whenComplete(($, throwable) -> {
			pendingAsyncOperation = false;
			if (flushStage != null) flushStage.setError(e);
		});
	}

	@Override
	public String toString() {
		return "StreamFileWriter{" +
				"asyncFile=" + asyncFile +
				", position=" + position +
				'}';
	}
	// endregion
}
