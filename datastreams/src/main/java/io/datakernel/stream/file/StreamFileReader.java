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
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;

/**
 * This producer allows you to read data from a file in a non-blocking fashion.
 */
public final class StreamFileReader extends AbstractStreamProducer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(StreamFileReader.class);

	public static final OpenOption[] READ_OPTIONS = new OpenOption[]{READ};

	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(8);

	private final AsyncFile asyncFile;

	private boolean reading = false;

	private int bufferSize = DEFAULT_BUFFER_SIZE.toInt();
	private long position = 0;
	private long length = Long.MAX_VALUE;

	private StreamFileReader(AsyncFile asyncFile) {
		this.asyncFile = asyncFile;
	}

	public static StreamFileReader readFile(ExecutorService executor, Path path) throws IOException {
		return new StreamFileReader(AsyncFile.open(executor, path, READ_OPTIONS));
	}

	public static StreamFileReader readFile(AsyncFile asyncFile) {
		return new StreamFileReader(asyncFile);
	}

	public StreamFileReader withBufferSize(MemSize bufferSize) {
		return withBufferSize(bufferSize.toInt());
	}

	public StreamFileReader withBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		checkArgument(bufferSize > 0, "Buffer size cannot be less than or equal to zero");
		return this;
	}

	public StreamFileReader withStartingPosition(long position) {
		checkState(getConsumer() == null, "Cannot set position after binding the reader");
		checkArgument(position >= 0, "Position cannot be less than zero");
		this.position = position;
		return this;
	}

	public StreamFileReader withReadingLength(long length) {
		checkState(getConsumer() == null, "Cannot set reading length after binding the reader");
		checkArgument(length >= -1, "Reading length cannot be less than -1");
		this.length = length == -1 ? Long.MAX_VALUE : length;
		return this;
	}

	public long getPosition() {
		return position;
	}

	private void process() {
		if (!isReceiverReady()) {
			reading = false;
			return;
		}
		ByteBuf buf = ByteBufPool.allocate((int) min(bufferSize, length));
		asyncFile.read(buf, position)
				.whenComplete((bytesRead, e) -> {
					if (e != null) {
						buf.recycle();
						closeWithError(e);
					} else if (bytesRead == -1) {
						buf.recycle();
						sendEndOfStream();
					} else {
						send(buf);
						position += bytesRead;
						if (length != Long.MAX_VALUE) {
							length -= bytesRead;
						}
						if (length == 0L) {
							sendEndOfStream();
						}
					}
					process();
				});
	}

	@Override
	protected void onProduce(StreamDataReceiver<ByteBuf> receiver) {
		if (!reading) {
			reading = true;
			process();
		}
	}

	@Override
	protected void cleanup() {
		asyncFile.close()
				.whenComplete(($, e) -> {
					if (e == null) {
						logger.trace(this + ": closed file");
					} else {
						logger.error(this + ": failed to close file", e);
					}
				});
	}

	@Override
	protected void onError(Throwable t) {
	}

	@Override
	public String toString() {
		return "StreamFileReader{" + asyncFile +
				", pos=" + position +
				(length == Long.MAX_VALUE ? "" : ", len=" + length) +
				(reading ? ", reading" : "") +
				'}';
	}
}
