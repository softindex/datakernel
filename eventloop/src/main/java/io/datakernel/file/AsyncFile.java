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

package io.datakernel.file;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.*;

/**
 * An abstract representation of file. Actions with this file are non-blocking
 */
public final class AsyncFile {
	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final AsynchronousFileChannel channel;

	private final Path path;

	/**
	 * Creates a new instance of AsyncFile
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  executor for running tasks in other thread
	 * @param channel   an asynchronous channel for reading, writing, and manipulating a file.
	 * @param path      path of the file
	 */
	private AsyncFile(Eventloop eventloop, ExecutorService executor, AsynchronousFileChannel channel, Path path) {
		this.eventloop = checkNotNull(eventloop);
		this.executor = checkNotNull(executor);
		this.channel = checkNotNull(channel);
		this.path = checkNotNull(path);
	}

	/**
	 * Opens file in a blocking manner
	 *
	 * @param eventloop   event loop in which a file will be used
	 * @param executor    executor for running tasks in other thread
	 * @param path        the  path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static AsyncFile open(final Eventloop eventloop, final ExecutorService executor,
	                             final Path path, final OpenOption[] openOptions) throws IOException {
		AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, new HashSet<>(Arrays.asList(openOptions)), executor);
		return new AsyncFile(eventloop, executor, channel, path);
	}

	/**
	 * Asynchronous opens file
	 *
	 * @param eventloop   event loop in which a file will be used
	 * @param executor    executor for running tasks in other thread
	 * @param path        the  path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static CompletionStage<AsyncFile> openAsync(final Eventloop eventloop, final ExecutorService executor,
	                                                   final Path path, final OpenOption[] openOptions) {
		return eventloop.callConcurrently(executor, () -> open(eventloop, executor, path, openOptions));
	}

	/**
	 * Deletes the file in new thread
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param path     the  path of the file to open or create
	 */
	public static CompletionStage<Void> delete(Eventloop eventloop, ExecutorService executor,
	                                           final Path path) {
		return eventloop.callConcurrently(executor, () -> {
			Files.delete(path);
			return null;
		});
	}

	public static CompletionStage<Long> length(Eventloop eventloop, ExecutorService executor, final Path path) {
		return eventloop.callConcurrently(executor, () -> {
			final File file = path.toFile();
			return !file.exists() || file.isDirectory() ? -1L : file.length();
		});
	}

	/**
	 * Moves or renames a file to a target file.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param source   the path to the file to move
	 * @param target    the path to the target file (may be associated with a different provider to the source path)
	 * @param options   options specifying how the move should be done
	 */
	public static CompletionStage<Void> move(Eventloop eventloop, ExecutorService executor,
	                                         final Path source, final Path target, final CopyOption... options) {
		return eventloop.callConcurrently(executor, () -> {
			Files.move(source, target, options);
			return null;
		});
	}

	/**
	 * Creates a new directory.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param dir      the directory to create
	 * @param attrs     an optional list of file attributes to set atomically when creating the directory
	 */
	public static CompletionStage<Void> createDirectory(Eventloop eventloop, ExecutorService executor,
	                                                    final Path dir, @Nullable final FileAttribute<?>[] attrs) {
		return eventloop.callConcurrently(executor, () -> {
			Files.createDirectory(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			return null;
		});
	}

	/**
	 * Creates a directory by creating all nonexistent parent directories first.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param dir      the directory to create
	 * @param attrs     an optional list of file attributes to set atomically when creating the directory
	 */
	public static CompletionStage<Void> createDirectories(Eventloop eventloop, ExecutorService executor,
	                                                      final Path dir, @Nullable final FileAttribute<?>[] attrs) {
		return eventloop.callConcurrently(executor, () -> {
			Files.createDirectories(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			return null;
		});
	}

	/**
	 * Reads all sequence of bytes from this channel into the given buffer.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param path     the  path of the file to read
	 */
	public static CompletionStage<ByteBuf> readFile(Eventloop eventloop, ExecutorService executor, Path path) {
		return openAsync(eventloop, executor, path, new OpenOption[]{READ}).thenCompose(
				file -> file.readFully().whenComplete((byteBuf, throwable) -> file.close()));
	}

	/**
	 * Creates new file and writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param path     the  path of the file to create and write
	 * @param buf       the  buffer from which bytes are to be transferred byteBuffer
	 */
	public static CompletionStage<Void> createNewAndWriteFile(Eventloop eventloop, ExecutorService executor,
	                                                          Path path, final ByteBuf buf) {
		return openAsync(eventloop, executor, path, new OpenOption[]{WRITE, CREATE_NEW}).thenCompose(
				file -> file.writeFully(buf, 0L).whenComplete(($, throwable) -> buf.recycle()));
	}

	/**
	 * Writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position.
	 *
	 * @param buf      the  buffer from which bytes are to be transferred
	 * @param position the  file position at which the transfer is to begin; must be non-negative
	 */
	public CompletionStage<Integer> write(final ByteBuf buf, long position) {
		eventloop.startConcurrentOperation();
		final ByteBuffer byteBuffer = buf.toReadByteBuffer();
		final SettableStage<Integer> stage = SettableStage.create();
		channel.write(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(final Integer result, Object attachment) {
				buf.ofReadByteBuffer(byteBuffer);
				eventloop.execute(() -> {
					eventloop.completeConcurrentOperation();
					stage.set(result);
				});
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(() -> {
					eventloop.completeConcurrentOperation();
					stage.setException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
				});
			}
		});
		return stage;
	}

	/**
	 * Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
	 *
	 * @param buf      the  buffer into which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin; must be non-negative
	 */
	public CompletionStage<Integer> read(final ByteBuf buf, long position) {
		eventloop.startConcurrentOperation();
		final ByteBuffer byteBuffer = buf.toWriteByteBuffer();
		final SettableStage<Integer> stage = SettableStage.create();
		channel.read(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(final Integer bytesRead, Object attachment) {
				buf.ofWriteByteBuffer(byteBuffer);
				eventloop.execute(() -> {
					eventloop.completeConcurrentOperation();
					stage.set(bytesRead);
				});
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(() -> {
					eventloop.completeConcurrentOperation();
					stage.setException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
				});
			}
		});
		return stage;
	}

	private CompletionStage<Void> writeFully(final ByteBuf buf, final long position,
	                                         final AtomicBoolean cancelled) {
		final ByteBuffer byteBuffer = buf.toReadByteBuffer();
		final SettableStage<Void> stage = SettableStage.create();
		channel.write(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer result, Object attachment) {
				buf.ofReadByteBuffer(byteBuffer);
				if (buf.readRemaining() == 0) {
					eventloop.execute(() -> {
						buf.recycle();
						eventloop.completeConcurrentOperation();
						stage.set(null);
					});
				} else {
					if (cancelled.get()) {
						eventloop.completeConcurrentOperation();
						return;
					}
					writeFully(buf, position + result, cancelled).whenComplete(($, throwable) -> {
						// TODO: improve
						if (throwable != null) {
							stage.setException(throwable);
						} else {
							stage.set(null);
						}
					});
				}
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(() -> {
					buf.recycle();
					eventloop.completeConcurrentOperation();
					stage.setException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
				});
			}
		});

		return stage;
	}

	/**
	 * Writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position. Writes in other thread.
	 *
	 * @param byteBuf  the  buffer from which bytes are to be transferred
	 * @param position the  file position at which the transfer is to begin; must be non-negative
	 */
	public CompletionStage<Void> writeFully(ByteBuf byteBuf, long position) {
		eventloop.startConcurrentOperation();
		AtomicBoolean cancelled = new AtomicBoolean();
		// TODO: add cancel logic
		return writeFully(byteBuf, position, cancelled);
	}

	private CompletionStage<Void> readFully(final ByteBuf buf, final long position, final long size,
	                                        final AtomicBoolean cancelled) {
		final SettableStage<Void> stage = SettableStage.create();
		final ByteBuffer byteBuffer = buf.toWriteByteBuffer();
		channel.read(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer result, Object attachment) {
				buf.ofWriteByteBuffer(byteBuffer);
				if (buf.readRemaining() == size || result == -1) {
					eventloop.execute(() -> {
						try {
							channel.close();
							eventloop.completeConcurrentOperation();
							stage.set(null);
						} catch (IOException e) {
							eventloop.completeConcurrentOperation();
							stage.setException(e);
						}

					});
				} else {
					if (cancelled.get()) {
						try {
							channel.close();
						} catch (IOException ignore) {
						}
						eventloop.completeConcurrentOperation();
						return;
					}
					readFully(buf, position, size, cancelled).whenComplete(($, throwable) -> {
						// TODO: improve
						if (throwable != null) {
							stage.setException(throwable);
						} else {
							stage.set(null);
						}
					});
				}
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(() -> {
					try {
						channel.close();
					} catch (IOException ignore) {
					}
					eventloop.completeConcurrentOperation();
					stage.setException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
				});
			}
		});

		return stage;
	}

	/**
	 * Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
	 * Reads in other thread.
	 *
	 * @param buf      the  buffer into which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin; must be non-negative
	 */
	public CompletionStage<Void> readFully(ByteBuf buf, long position) {
		long size;

		try {
			size = channel.size();
		} catch (IOException e) {
			return Stages.ofException(e);
		}

		eventloop.startConcurrentOperation();
		final AtomicBoolean cancelled = new AtomicBoolean();
		// TODO: add cancel logic
		return readFully(buf, position, size, cancelled);
	}

	/**
	 * Reads all sequence of bytes from this channel into buffer and sends this buffer to {@code stage}
	 */
	public CompletionStage<ByteBuf> readFully() {
		long size;

		try {
			size = channel.size();
		} catch (IOException e) {
			return Stages.ofException(e);
		}

		final ByteBuf buf = ByteBufPool.allocate((int) size);
		return readFully(buf, 0).whenComplete(($, throwable) -> {
			if (throwable != null) buf.recycle();
		}).thenApply($ -> buf);
	}

	public CompletionStage<Void> forceAndClose() {
		return eventloop.callConcurrently(executor, () -> {
			channel.force(true);
			channel.close();
			return null;
		});
	}

	/**
	 * Closes the channel
	 */
	public CompletionStage<Void> close() {
		return eventloop.callConcurrently(executor, () -> {
			channel.close();
			return null;
		});
	}

	/**
	 * Truncates this file to the given size.
	 *
	 * @param size the new size, a non-negative byte count
	 */
	public CompletionStage<Void> truncate(final long size) {
		return eventloop.callConcurrently(executor, () -> {
			channel.truncate(size);
			return null;
		});
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it.
	 *
	 * @param metaData if true then this method is required to force changes to both the file's
	 *                 content and metadata to be written to storage; otherwise, it need only force content changes to be written
	 */
	public CompletionStage<Void> force(final boolean metaData) {
		return eventloop.callConcurrently(executor, () -> {
			channel.force(metaData);
			return null;
		});
	}

	public Eventloop getEventloop() {
		return eventloop;
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public AsynchronousFileChannel getChannel() {
		return channel;
	}

	public boolean isOpen() {
		return channel.isOpen();
	}

	@Override
	public String toString() {
		return path.toString();
	}
}
