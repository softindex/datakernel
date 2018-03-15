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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamDataReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.*;

/**
 * This consumer allows you to write data to file in a non-blocking fashion.
 */
public final class StreamFileWriter extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(StreamFileWriter.class);

	public static final OpenOption[] CREATE_OPTIONS = new OpenOption[]{WRITE, CREATE, TRUNCATE_EXISTING};

	private final Deque<ByteBuf> bufs = new ArrayDeque<>();
	private final SettableStage<Void> flushStage = SettableStage.create();

	private final AsyncFile asyncFile;

	private boolean writing = false;

	private boolean forceOnClose = false;
	private boolean forceMetadata = false;
	private long maxBufferedBytes = 0;
	private int maxBuffers = 1;

	private long bufferedBytes = 0;
	private long position = 0;

	private StreamFileWriter(AsyncFile asyncFile) {
		this.asyncFile = asyncFile;
	}

	public static StreamFileWriter create(ExecutorService executor, Path path) throws IOException {
		return create(AsyncFile.open(executor, path, CREATE_OPTIONS));
	}

	public static StreamFileWriter create(AsyncFile asyncFile) {
		return new StreamFileWriter(asyncFile);
	}

	public StreamConsumerWithResult<ByteBuf, Void> withFlushAsResult() {
		return withResult(flushStage);
	}

	public StreamFileWriter withForceOnClose(boolean forceMetadata) {
		forceOnClose = true;
		this.forceMetadata = forceMetadata;
		return this;
	}

	public StreamFileWriter withMaxBufferedBytes(long bytes) {
		maxBufferedBytes = bytes;
		return this;
	}

	public StreamFileWriter withMaxBuffers(int buffers) {
		maxBuffers = buffers;
		return this;
	}

	public Stage<Void> getFlushStage() {
		return flushStage;
	}

	private void process() {
		ByteBuf data = bufs.poll();
		if (data == null) {
			if (getStatus().isClosed()) {
				close();
				return;
			}
			writing = false;
			getProducer().produce(this);
			return;
		}
		int length = data.readRemaining();
		asyncFile.writeFully(data, position)
				.whenComplete(($, e) -> {
					if (e != null) {
						closeWithError(e);
						return;
					}
					position += length;
					bufferedBytes -= length;
					if (bufs.size() <= maxBuffers || bufferedBytes <= 0) {
						getProducer().produce(this);
					}
					process();
				});
	}

	private void step() {
		if (!writing) {
			writing = true;
			process();
		}
	}

	@Override
	public void onData(ByteBuf buf) {
		if (getStatus().isClosed()) {
			buf.recycle();
			return;
		}
		bufs.offer(buf);
		bufferedBytes += buf.readRemaining();
		if (!bufs.isEmpty() && (bufs.size() > maxBuffers || bufferedBytes > maxBufferedBytes)) {
			getProducer().suspend();
		}
		step();
	}

	private void close() {
		for (ByteBuf buf : bufs) {
			buf.recycle();
		}
		bufs.clear();
		(forceOnClose ?
				asyncFile.forceAndClose(forceMetadata) :
				asyncFile.close())
				.whenComplete(flushStage::trySet)
				.whenComplete(($, e) -> {
					if (e == null) {
						logger.info(this + ": closed file");
					} else {
						logger.error(this + ": failed to close file", e);
					}
				});
	}

	@Override
	protected void onStarted() {
		getProducer().produce(this);
	}

	@Override
	protected void onEndOfStream() {
		step();
	}

	@Override
	protected void onError(Throwable t) {
		flushStage.setException(t);
		close();
	}

	@Override
	public String toString() {
		return "StreamFileWriter{" +
				"asyncFile=" + asyncFile +
				", position=" + position +
				(writing ? ", writing" : "") +
				'}';
	}
}
