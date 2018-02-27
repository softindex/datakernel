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
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Arrays.asList;

/**
 * An representation of file with non-blocking operations.
 */
public final class AsyncFile {
	private final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final ExecutorService executor;
	private final AsynchronousFileChannel channel;

	private final Path path;

	/**
	 * Creates a new instance of AsyncFile
	 *
	 * @param executor executor for running tasks in other thread
	 * @param channel  an asynchronous channel for reading, writing, and manipulating a file.
	 * @param path     path of the file
	 */
	private AsyncFile(ExecutorService executor, AsynchronousFileChannel channel, Path path) {
		this.executor = checkNotNull(executor);
		this.channel = checkNotNull(channel);
		this.path = checkNotNull(path);
	}

	/**
	 * Opens file in a blocking manner
	 *
	 * @param executor    executor for running tasks in other thread
	 * @param path        the path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static AsyncFile open(ExecutorService executor, Path path, OpenOption[] openOptions) throws IOException {
		AsynchronousFileChannel channel = doOpenChannel(executor, path, openOptions);
		return new AsyncFile(executor, channel, path);
	}

	/**
	 * Asynchronous opens file
	 *
	 * @param executor    executor for running tasks in other thread
	 * @param path        the path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static Stage<AsyncFile> openAsync(ExecutorService executor, Path path, OpenOption[] openOptions) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		return Stage.ofCallable(executor, () -> doOpenChannel(executor, path, openOptions))
				.thenApply(channel -> new AsyncFile(executor, channel, path));
	}

	private static AsynchronousFileChannel doOpenChannel(ExecutorService executor, Path path, OpenOption[] openOptions) throws IOException {
		return AsynchronousFileChannel.open(path, new HashSet<>(asList(openOptions)), executor);
	}

	/**
	 * Deletes the file in new thread
	 *
	 * @param path the path of the file to open or create
	 */
	public static Stage<Void> delete(ExecutorService executor, Path path) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		return Stage.ofCallable(executor, () -> {
			Files.delete(path);
			return null;
		});
	}

	public static Stage<Long> size(ExecutorService executor, Path path) {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		return Stage.ofCallable(executor, () -> Files.isRegularFile(path) ? Files.size(path) : -1L);
	}

	/**
	 * Moves or renames a file to a target file.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param source    the path to the file to move
	 * @param target    the path to the target file (may be associated with a different provider to the source path)
	 * @param options   options specifying how the move should be done
	 */
	public static Stage<Void> move(Eventloop eventloop, ExecutorService executor, Path source, Path target, CopyOption... options) {
		return Stage.ofCallable(executor, () -> {
			Files.move(source, target, options);
			return null;
		});
	}

	/**
	 * Creates a new directory.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param dir       the directory to create
	 * @param attrs     an optional list of file attributes to set atomically when creating the directory
	 */
	public static Stage<Void> createDirectory(Eventloop eventloop, ExecutorService executor, Path dir, @Nullable FileAttribute<?>[] attrs) {
		return Stage.ofCallable(executor, () -> {
			Files.createDirectory(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			return null;
		});
	}

	/**
	 * Creates a directory by creating all nonexistent parent directories first.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param dir       the directory to create
	 * @param attrs     an optional list of file attributes to set atomically when creating the directory
	 */
	public static Stage<Void> createDirectories(Eventloop eventloop, ExecutorService executor, Path dir, @Nullable FileAttribute<?>[] attrs) {
		return Stage.ofCallable(executor, () -> {
			Files.createDirectories(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			return null;
		});
	}

	/**
	 * Reads all sequence of bytes from this channel into the given buffer.
	 *
	 * @param path the path of the file to read
	 */
	public static Stage<ByteBuf> readFile(ExecutorService executor, Path path) {
		return openAsync(executor, path, new OpenOption[]{READ})
				.thenCompose(file -> file.readFully().whenComplete(($, e) -> file.close()));
	}

	/**
	 * Creates new file and writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position.
	 * If file exists then stage fails with exception.
	 *
	 * @param path the path of the file to create and write
	 * @param buf  the buffer from which bytes are to be transferred byteBuffer
	 */
	public static Stage<Void> writeNewFile(ExecutorService executor, Path path, ByteBuf buf) {
		return openAsync(executor, path, new OpenOption[]{WRITE, CREATE_NEW})
				.thenCompose(file -> file.writeFully(buf, 0L)
						.whenComplete(($, throwable) -> buf.recycle()));
	}

	/**
	 * Writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position.
	 *
	 * @param buf      the buffer from which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin; must be non-negative
	 */
	public Stage<Integer> write(ByteBuf buf, long position) {
		eventloop.startExternalTask();
		ByteBuffer byteBuffer = buf.toReadByteBuffer();
		SettableStage<Integer> stage = SettableStage.create();
		channel.write(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer result, Object $) {
				buf.ofReadByteBuffer(byteBuffer);
				eventloop.execute(() -> {
					eventloop.completeExternalTask();
					stage.set(result);
				});
			}

			@Override
			public void failed(Throwable exc, Object $) {
				eventloop.execute(() -> {
					eventloop.completeExternalTask();
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
	public Stage<Integer> read(ByteBuf buf, long position) {
		eventloop.startExternalTask();
		ByteBuffer byteBuffer = buf.toWriteByteBuffer();
		SettableStage<Integer> stage = SettableStage.create();
		channel.read(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer bytesRead, Object $) {
				buf.ofWriteByteBuffer(byteBuffer);
				eventloop.execute(() -> {
					eventloop.completeExternalTask();
					stage.set(bytesRead);
				});
			}

			@Override
			public void failed(Throwable exc, Object $) {
				eventloop.execute(() -> {
					eventloop.completeExternalTask();
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
	 * @param buf      the  buffer from which bytes are to be transferred
	 * @param position the  file position at which the transfer is to begin; must be non-negative
	 */
	public Stage<Void> writeFully(ByteBuf buf, long position) {
		ByteBuffer byteBuffer = buf.toReadByteBuffer();
		SettableStage<Void> stage = SettableStage.create();
		eventloop.startExternalTask();
		channel.write(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer writtenBytes, Object $) {
				buf.ofReadByteBuffer(byteBuffer);
				if (buf.readRemaining() == 0) {
					eventloop.execute(() -> {
						eventloop.completeExternalTask();
						buf.recycle();
						stage.set(null);
					});
					return;
				}
				writeFully(buf, position + writtenBytes).whenComplete(stage::set);
			}

			@Override
			public void failed(Throwable e, Object $) {
				eventloop.execute(() -> {
					buf.recycle();
					eventloop.completeExternalTask();
					stage.setException(e instanceof Exception ? (Exception) e : new Exception(e));
				});
			}
		});
		return stage;
	}

	/**
	 * Asynchronously reads all bytes from this channel into a buffer.
	 */
	public Stage<ByteBuf> readFully() {
		return readFully(0);
	}

	/**
	 * Asynchronously reads all bytes from this channel into a buffer.
	 */
	public Stage<ByteBuf> readFully(long position) {
		long size;

		try {
			size = channel.size();
		} catch (IOException e) {
			return Stage.ofException(e);
		}

		ByteBuf buf = ByteBufPool.allocate((int) (size - position));
		return readFully(buf, position, size)
				.whenComplete(($, e) -> {
					if (e != null) {
						buf.recycle();
					}
				})
				.thenApply($ -> buf);
	}

	/**
	 * Asynchronously reads a sequence of bytes from this channel into the given buffer,
	 * starting at the given file position.
	 * Reads are happenning in other thread(s).
	 *
	 * @param buf      the buffer into which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin; must be non-negative
	 */
	public Stage<Void> readFully(ByteBuf buf, long position) {
		long size;

		try {
			size = channel.size();
		} catch (IOException e) {
			return Stage.ofException(e);
		}
		return readFully(buf, position, size);
	}

	private Stage<Void> readFully(ByteBuf buf, long position, long size) {
		SettableStage<Void> stage = SettableStage.create();
		ByteBuffer byteBuffer = buf.toWriteByteBuffer();
		eventloop.startExternalTask();
		channel.read(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer result, Object $) {
				buf.ofWriteByteBuffer(byteBuffer);
				if (buf.readRemaining() == size || result == -1) {
					eventloop.execute(() -> {
						eventloop.completeExternalTask();
						try {
							channel.close();
							stage.set(null);
						} catch (IOException e) {
							stage.setException(e);
						}

					});
					return;
				}
				readFully(buf, position + result, size - result).whenComplete(stage::set);
			}

			@Override
			public void failed(Throwable e, Object $) {
				eventloop.execute(() -> {
					try {
						channel.close();
					} catch (IOException ignore) {
					}
					eventloop.completeExternalTask();
					stage.setException(e instanceof Exception ? (Exception) e : new Exception(e));
				});
			}
		});

		return stage;
	}

	/**
	 * Forces physical write and then closes the channel
	 *
	 * @param forceMetadata whether or not to force metadata writes too
	 */
	public Stage<Void> forceAndClose(boolean forceMetadata) {
		return Stage.ofCallable(executor, () -> {
			channel.force(forceMetadata);
			channel.close();
			return null;
		});
	}

	/**
	 * Closes the channel
	 */
	public Stage<Void> close() {
		return Stage.ofCallable(executor, () -> {
			channel.close();
			return null;
		});
	}

	/**
	 * Truncates this file to the given size.
	 *
	 * @param size the new size, a non-negative byte count
	 */
	public Stage<Void> truncate(long size) {
		return Stage.ofCallable(executor, () -> {
			channel.truncate(size);
			return null;
		});
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it.
	 *
	 * @param metaData if true then this method is required to force changes of both
	 *                 file content and metadata to be written to storage;
	 *                 otherwise, it need only force content changes to be written
	 */
	public Stage<Void> force(boolean metaData) {
		return Stage.ofCallable(executor, () -> {
			channel.force(metaData);
			return null;
		});
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
