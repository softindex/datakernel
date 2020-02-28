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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.common.MemSize;
import io.datakernel.common.exception.CloseException;
import io.datakernel.csp.AbstractChannelSupplier;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.Executor;

import static io.datakernel.common.Preconditions.checkArgument;
import static java.nio.file.StandardOpenOption.READ;

/**
 * This supplier allows you to asynchronously read binary data from a file.
 */
public final class ChannelFileReader extends AbstractChannelSupplier<ByteBuf> {
	private static final Logger logger = LoggerFactory.getLogger(ChannelFileReader.class);

	public static final OpenOption[] DEFAULT_OPTIONS = new OpenOption[]{READ};

	public static final MemSize DEFAULT_BUFFER_SIZE = MemSize.kilobytes(8);

	private final AsyncFileService fileService;
	private final FileChannel channel;

	private int bufferSize = DEFAULT_BUFFER_SIZE.toInt();
	private long position = 0;
	private long limit = Long.MAX_VALUE;

	private ChannelFileReader(AsyncFileService fileService, FileChannel channel) {
		this.fileService = fileService;
		this.channel = channel;
	}

	public static ChannelFileReader create(Executor executor, FileChannel channel) {
		return create(new ExecutorAsyncFileService(executor), channel);
	}

	public static ChannelFileReader create(AsyncFileService fileService, FileChannel channel) {
		return new ChannelFileReader(fileService, channel);
	}

	public static Promise<ChannelFileReader> open(Executor executor, Path path) {
		return Promise.ofBlockingCallable(executor, () -> openBlocking(executor, path, DEFAULT_OPTIONS));
	}

	public static Promise<ChannelFileReader> open(Executor executor, Path path, OpenOption... openOptions) {
		return Promise.ofBlockingCallable(executor, () -> openBlocking(executor, path, openOptions));
	}

	public static ChannelFileReader openBlocking(Executor executor, Path path, OpenOption... openOptions) throws IOException {
		FileChannel channel = FileChannel.open(path, openOptions);
		return new ChannelFileReader(new ExecutorAsyncFileService(executor), channel);
	}

	public static ChannelFileReader openBlocking(Executor executor, Path path) throws IOException {
		FileChannel channel = FileChannel.open(path, DEFAULT_OPTIONS);
		return new ChannelFileReader(new ExecutorAsyncFileService(executor), channel);
	}

	public ChannelFileReader withBufferSize(MemSize bufferSize) {
		return withBufferSize(bufferSize.toInt());
	}

	public ChannelFileReader withBufferSize(int bufferSize) {
		checkArgument(bufferSize > 0, "Buffer size cannot be less than or equal to zero");
		this.bufferSize = bufferSize;
		return this;
	}

	public ChannelFileReader withOffset(long offset) {
		checkArgument(offset >= 0, "Offset cannot be negative");
		position = offset;
		return this;
	}

	public ChannelFileReader withLength(long length) {
		checkArgument(length >= 0, "Length cannot be less than zero");
		this.limit = length;
		return this;
	}

	public long getPosition() {
		return position;
	}

	@Override
	protected Promise<ByteBuf> doGet() {
		if (limit == 0) {
			close();
			return Promise.of(null);
		}
		ByteBuf buf = ByteBufPool.allocateExact((int) Math.min(bufferSize, limit));
		return fileService.read(channel, position, buf.array(), buf.head(), buf.writeRemaining()) // reads are synchronized at least on asyncFile, so if produce() is called twice, position wont be broken (i hope)
				.thenEx((bytesRead, e) -> {
					if (e != null) {
						buf.recycle();
						closeEx(e);
						return Promise.ofException(getException());
					}
					if (bytesRead == 0) { // no data read, assuming end of file
						buf.recycle();
						close();
						return Promise.of(null);
					}

					buf.moveTail(Math.toIntExact(bytesRead));
					position += bytesRead;
					if (limit != Long.MAX_VALUE) {
						limit -= bytesRead; // bytesRead is always <= the limit (^ see the min call)
					}
					return Promise.of(buf);
				});
	}

	@Override
	protected void onClosed(@NotNull Throwable e) {
		try {
			if (!channel.isOpen()) {
				throw new CloseException(ChannelFileReader.class, "File has been closed");
			}

			channel.close();
			logger.trace(this + ": closed file");
		} catch (IOException | CloseException e1) {
			logger.error(this + ": failed to close file", e1);
		}
	}

	@Override
	public String toString() {
		return "ChannelFileReader{" +
				"pos=" + position +
				(limit == Long.MAX_VALUE ? "" : ", len=" + limit) +
				'}';
	}
}
