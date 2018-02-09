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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;

import static java.lang.Math.min;

/**
 * This class allows you to read data from file non-blocking. It represents a {@link AbstractStreamProducer}
 * which streams data from file.
 */
public final class StreamFileReader extends AbstractStreamProducer<ByteBuf> {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final AsyncFile asyncFile;

	private final int bufferSize;
	private long position;
	private long length;
	private boolean pendingAsyncOperation;

	// region creators
	private StreamFileReader(AsyncFile asyncFile,
	                         int bufferSize, long position, long length) {
		this.asyncFile = asyncFile;
		this.bufferSize = bufferSize;
		this.position = position;
		this.length = length;
	}

	public static StreamFileReader readFileSegment(AsyncFile asyncFile,
	                                               int bufferSize, long position, long length) {
		return new StreamFileReader(asyncFile, bufferSize, position, length);
	}

	public static StreamFileReader readFileFrom(ExecutorService executor,
	                                            int bufferSize, Path path,
	                                            long startPosition) throws IOException {
		AsyncFile asyncFile = getAsyncFile(executor, path);
		return readFileFrom(asyncFile, bufferSize, startPosition);
	}

	public static StreamFileReader readFileFrom(AsyncFile asyncFile,
	                                            int bufferSize, long position) {
		return new StreamFileReader(asyncFile, bufferSize, position, Long.MAX_VALUE);
	}

	public static StreamFileReader readFileFully(ExecutorService executor,
	                                             int bufferSize, Path path) throws IOException {
		AsyncFile asyncFile = getAsyncFile(executor, path);
		return readFileFully(asyncFile, bufferSize);
	}

	public static StreamFileReader readFileFully(AsyncFile asyncFile, int bufferSize) {
		return new StreamFileReader(asyncFile, bufferSize, 0, Long.MAX_VALUE);
	}
	// endregion

	// region api
	public long getPosition() {
		return position;
	}
	// endregion

	// region functional
	protected void doFlush() {
		if (getStatus().isClosed()) {
			return;
		}

		if (length == 0L) {
			sendEndOfStream();
			return;
		}

		ByteBuf buf = ByteBufPool.allocate((int) min(bufferSize, length));

		asyncFile.read(buf, position).whenComplete((bytesRead, throwable) -> {
			pendingAsyncOperation = false;
			if (throwable == null) {
				if (getStatus().isClosed()) {
					buf.recycle();
					return;
				}
				if (bytesRead == -1) {
					buf.recycle();
					sendEndOfStream();
					return;
				} else {
					position += bytesRead;
					getLastDataReceiver().onData(buf);
					if (length != Long.MAX_VALUE) {
						length -= bytesRead;
					}
				}
				if (isReceiverReady()) {
					postFlush();
				}
			} else {
				buf.recycle();
				closeWithError(throwable);
			}
		});
	}

	protected void postFlush() {
		if (!pendingAsyncOperation) {
			pendingAsyncOperation = true;
			eventloop.post(this::doFlush);
		}
	}

	@Override
	protected void onProduce(StreamDataReceiver<ByteBuf> dataReceiver) {
		postFlush();
	}

	@Override
	protected void onError(Throwable t) {
	}

	@Override
	protected void cleanup() {
		asyncFile.close()
				.whenException(throwable -> logger.error("{}: failed to close file", this, throwable));
	}

	private static AsyncFile getAsyncFile(ExecutorService executor, Path path) throws IOException {
		return AsyncFile.open(executor, path, new OpenOption[]{StandardOpenOption.READ});
	}

	@Override
	public String toString() {
		return "StreamFileReader{" + asyncFile +
				", pos=" + position +
				(length == Long.MAX_VALUE ? "" : ", len=" + length) +
				(pendingAsyncOperation ? ", pendingAsyncOperation" : "") +
				'}';
	}
	// endregion
}

