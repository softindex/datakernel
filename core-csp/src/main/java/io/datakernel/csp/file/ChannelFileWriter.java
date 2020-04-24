/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.csp.file;

import io.datakernel.async.file.AsyncFileService;
import io.datakernel.async.file.ExecutorAsyncFileService;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.AbstractChannelConsumer;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static java.nio.file.StandardOpenOption.*;

/**
 * This consumer allows you to asynchronously write binary data to a file.
 */
public final class ChannelFileWriter extends AbstractChannelConsumer<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileWriter.class);

	public static final OpenOption[] DEFAULT_OPTIONS = new OpenOption[]{WRITE, CREATE_NEW, APPEND};

	private final AsyncFileService fileService;
	private final FileChannel channel;

	private boolean forceOnClose = false;
	private boolean forceMetadata = false;
	private long startingOffset = 0;
	private boolean started;

	private long position = 0;

	// region creators

	public ChannelFileWriter(AsyncFileService fileService, FileChannel channel) {
		this.fileService = fileService;
		this.channel = channel;
	}

	public static ChannelFileWriter create(Executor executor, FileChannel channel) {
		return create(new ExecutorAsyncFileService(executor), channel);
	}

	public static ChannelFileWriter create(ExecutorAsyncFileService asyncFileService, FileChannel channel) {
		return new ChannelFileWriter(asyncFileService, channel);
	}

	public static Promise<ChannelFileWriter> open(Executor executor, Path path) {
		return open(executor, path, DEFAULT_OPTIONS);
	}

	public static Promise<ChannelFileWriter> open(Executor executor, Path path, OpenOption... openOptions) {
		return Promise.ofBlockingCallable(executor, () -> openBlocking(executor, path, openOptions));
	}

	public static ChannelFileWriter openBlocking(Executor executor, Path path) throws IOException {
		FileChannel channel = FileChannel.open(path, DEFAULT_OPTIONS);
		return create(executor, channel);
	}

	public static ChannelFileWriter openBlocking(Executor executor, Path path, OpenOption... openOptions) throws IOException {
		FileChannel channel = FileChannel.open(path, openOptions);
		return create(executor, channel);
	}

	public ChannelFileWriter withForceOnClose(boolean forceMetadata) {
		forceOnClose = true;
		this.forceMetadata = forceMetadata;
		return this;
	}

	public ChannelFileWriter withOffset(long offset) {
		startingOffset = offset;
		return this;
	}
	// endregion

	public long getPosition() {
		return position;
	}

	@Override
	protected void onClosed(@NotNull Throwable e) {
		closeFile();
	}

	@Override
	protected Promise<Void> doAccept(ByteBuf buf) {
		if (!started) {
			position = startingOffset;
		}
		started = true;
		if (buf == null) {
			closeFile();
			close();
			return Promise.of(null);
		}
		long p = position;
		position += buf.readRemaining();

		byte[] array = buf.getArray();
		return fileService.write(channel, p, array, 0, array.length)
				.thenEx(($, e2) -> {
					if (isClosed()) return Promise.ofException(getException());
					if (e2 != null) {
						closeEx(e2);
					}
					return Promise.of($, e2);
				})
				.then(() -> {
					buf.recycle();
					return Promise.complete();
				});
	}

	private void closeFile() {
		if (!channel.isOpen()) {
			return;
		}

		try {
			if (forceOnClose) {
				channel.force(forceMetadata);
			}

			channel.close();
			logger.trace(this + ": closed file");
		} catch (IOException e) {
			logger.error(this + ": failed to close file", e);
		}
	}

	@Override
	public String toString() {
		return "ChannelFileWriter{}";
	}
}
