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
import io.datakernel.async.*;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.datakernel.async.AsyncCallbacks.*;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.*;

/**
 * An abstract representation of file. Actions with this file are non-blocking
 */
public final class AsyncFile implements File {
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
	 *  Opens file in a blocking manner
	 *
	 * @param eventloop   event loop in which a file will be used
	 * @param executor    executor for running tasks in other thread
	 * @param path        the  path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static AsyncFile open(final Eventloop eventloop, final ExecutorService executor,
	                             final Path path, final OpenOption[] openOptions) throws IOException {
		AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, openOptions);
		return new AsyncFile(eventloop, executor, channel, path);
	}

	/**
	 * Asynchronous opens file
	 *
	 * @param eventloop   event loop in which a file will be used
	 * @param executor    executor for running tasks in other thread
	 * @param path        the  path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 * @param callback    callback which will be called after opening
	 */
	@SuppressWarnings("unchecked")
	public static <T extends File> void open(final Eventloop eventloop, final ExecutorService executor,
	                                         final Path path, final OpenOption[] openOptions, ResultCallback<T> callback) {
		callConcurrently(eventloop, executor, false, new Callable<T>() {
			@Override
			public T call() throws Exception {
				AsynchronousFileChannel channel = AsynchronousFileChannel.open(path, openOptions);
				return (T) new AsyncFile(eventloop, executor, channel, path);
			}
		}, callback);
	}

	/**
	 * Deletes the file in new thread
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param path     the  path of the file to open or create
	 * @param callback  callback which will be called after opening
	 */
	public static void delete(Eventloop eventloop, ExecutorService executor,
	                          final Path path, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				Files.delete(path);
			}
		}, callback);
	}

	public static void length(Eventloop eventloop, ExecutorService executor, final Path path,
	                          ResultCallback<Long> callback) {
		callConcurrently(eventloop, executor, false, new Callable<Long>() {
			@Override
			public Long call() throws Exception {
				java.io.File file = path.toFile();
				if (!file.exists() || file.isDirectory()) {
					return -1L;
				} else {
					return file.length();
				}
			}
		}, callback);
	}

	/**
	 * Moves or renames a file to a target file.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param source   the path to the file to move
	 * @param target    the path to the target file (may be associated with a different provider to the source path)
	 * @param options   options specifying how the move should be done
	 * @param callback  callback which will be called after moving
	 */
	public static void move(Eventloop eventloop, ExecutorService executor,
	                        final Path source, final Path target, final CopyOption[] options, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				Files.move(source, target, options);
			}
		}, callback);
	}

	/**
	 * Creates a new directory.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param dir      the directory to create
	 * @param attrs     an optional list of file attributes to set atomically when creating the directory
	 * @param callback  callback which will be called after creating
	 */
	public static void createDirectory(Eventloop eventloop, ExecutorService executor,
	                                   final Path dir, @Nullable final FileAttribute<?>[] attrs, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				Files.createDirectory(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			}
		}, callback);
	}

	/**
	 * Creates a directory by creating all nonexistent parent directories first.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param dir      the directory to create
	 * @param attrs     an optional list of file attributes to set atomically when creating the directory
	 * @param callback  callback which will be called after creating
	 */
	public static void createDirectories(Eventloop eventloop, ExecutorService executor,
	                                     final Path dir, @Nullable final FileAttribute<?>[] attrs, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				Files.createDirectories(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			}
		}, callback);
	}

	/**
	 * Reads all sequence of bytes from this channel into the given buffer.
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param path     the  path of the file to read
	 * @param callback  which will be called after complete
	 */
	public static void readFile(Eventloop eventloop, ExecutorService executor,
	                            Path path, final ResultCallback<ByteBuf> callback) {
		open(eventloop, executor, path, new OpenOption[]{READ}, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			public void onResult(final AsyncFile file) {
				file.readFully(new ForwardingResultCallback<ByteBuf>(callback) {
					@Override
					public void onResult(ByteBuf buf) {
						file.close(ignoreCompletionCallback());
						callback.onResult(buf);
					}

					@Override
					public void onException(Exception exception) {
						file.close(ignoreCompletionCallback());
						callback.onException(exception);
					}
				});
			}
		});
	}

	/**
	 * Creates new file and writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position
	 *
	 * @param eventloop event loop in which a file will be used
	 * @param executor  @param path     the  path of the file to create and write
	 * @param buf       the  buffer from which bytes are to be transferred byteBuffer
	 * @param callback  which will be called after complete
	 */
	public static void createNewAndWriteFile(Eventloop eventloop, ExecutorService executor,
	                                         Path path, final ByteBuf buf, final CompletionCallback callback) {
		open(eventloop, executor, path, new OpenOption[]{WRITE, CREATE_NEW}, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			public void onResult(AsyncFile file) {
				file.writeFully(buf, 0L, new CompletionCallback() {
					@Override
					public void onComplete() {
						buf.recycle();
						callback.onComplete();
					}

					@Override
					public void onException(Exception exception) {
						buf.recycle();
						callback.onException(exception);
					}
				});
			}
		});
	}

	/**
	 * Writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position.
	 *
	 * @param buf      the  buffer from which bytes are to be transferred
	 * @param position the  file position at which the transfer is to begin; must be non-negative
	 * @param callback callback which will be called after complete
	 */
	@Override
	public void write(final ByteBuf buf, long position, final ResultCallback<Integer> callback) {
		final Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final ByteBuffer byteBuffer = buf.toByteBuffer();
		channel.write(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(final Integer result, Object attachment) {
				buf.setByteBuffer(byteBuffer);
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						tracker.complete();
						callback.onResult(result);
					}
				});
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						tracker.complete();
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
			}
		});
	}

	/**
	 * Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
	 *
	 * @param buf      the  buffer into which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin; must be non-negative
	 * @param callback which will be called after complete
	 */
	@Override
	public void read(final ByteBuf buf, long position, final ResultCallback<Integer> callback) {
		final Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final ByteBuffer byteBuffer = buf.toByteBuffer();
		channel.read(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(final Integer result, Object attachment) {
				buf.setByteBuffer(byteBuffer);
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						tracker.complete();
						callback.onResult(result);
					}
				});
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						tracker.complete();
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
			}
		});
	}

	private void writeFully(final ByteBuf buf, final long position, final Eventloop.ConcurrentOperationTracker tracker,
	                        final AtomicBoolean cancelled, final CompletionCallback callback) {
		final ByteBuffer byteBuffer = buf.toByteBuffer();
		channel.write(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer result, Object attachment) {
				buf.setByteBuffer(byteBuffer);
				if (buf.remaining() == 0) {
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							tracker.complete();
							callback.onComplete();
						}
					});
				} else {
					if (cancelled.get()) {
						tracker.complete();
						return;
					}
					writeFully(buf, position + result, tracker, cancelled, callback);
				}
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						tracker.complete();
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
			}
		});
	}

	/**
	 * Writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position. Writes in other thread.
	 *
	 * @param byteBuf  the  buffer from which bytes are to be transferred
	 * @param position the  file position at which the transfer is to begin; must be non-negative
	 * @param callback callback which will be called after complete
	 */
	@Override
	public void writeFully(ByteBuf byteBuf, long position, CompletionCallback callback) {
		Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final AtomicBoolean cancelled = new AtomicBoolean();
		writeFully(byteBuf, position, tracker, cancelled, callback);
		notifyOnCancel(callback, new AsyncCancellableStatus.CancelNotifier() {
			@Override
			public void onCancel() {
				cancelled.set(true);
			}
		});
	}

	private void readFully(final ByteBuf buf, final long position,
	                       final Eventloop.ConcurrentOperationTracker tracker,
	                       final AtomicBoolean cancelled, final CompletionCallback callback) {
		final ByteBuffer byteBuffer = buf.toByteBuffer();
		channel.read(byteBuffer, position, null, new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(Integer result, Object attachment) {
				buf.setByteBuffer(byteBuffer);
				if (buf.remaining() == 0 || result == -1) {
					eventloop.execute(new Runnable() {
						@Override
						public void run() {
							tracker.complete();
							callback.onComplete();
						}
					});
				} else {
					if (cancelled.get()) {
						tracker.complete();
						return;
					}
					readFully(buf, position, tracker, cancelled, callback);
				}
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.execute(new Runnable() {
					@Override
					public void run() {
						tracker.complete();
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
			}
		});
	}

	/**
	 * Reads a sequence of bytes from this channel into the given buffer, starting at the given file position.
	 * Reads in other thread.
	 *
	 * @param buf      the  buffer into which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin; must be non-negative
	 * @param callback which will be called after complete
	 */
	@Override
	public void readFully(ByteBuf buf, long position, CompletionCallback callback) {
		long size;

		try {
			size = channel.size();
		} catch (IOException e) {
			callback.onException(e);
			return;
		}

		buf.limit((int) size);
		Eventloop.ConcurrentOperationTracker tracker = eventloop.startConcurrentOperation();
		final AtomicBoolean cancelled = new AtomicBoolean();
		readFully(buf, position, tracker, cancelled, callback);
		notifyOnCancel(callback, new AsyncCancellableStatus.CancelNotifier() {
			@Override
			public void onCancel() {
				cancelled.set(true);
			}
		});
	}

	/**
	 * Reads all sequence of bytes from this channel into buffer and sends this buffer to {@code callback}
	 *
	 * @param callback which will be called after complete
	 */
	@Override
	public void readFully(final ResultCallback<ByteBuf> callback) {
		long size;

		try {
			size = channel.size();
		} catch (IOException e) {
			callback.onException(e);
			return;
		}

		final ByteBuf buf = ByteBufPool.allocate((int) size);
		buf.limit((int) size);
		readFully(buf, 0, new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				buf.flip();
				callback.onResult(buf);
			}

			@Override
			public void onException(Exception exception) {
				buf.recycle();
				callback.onException(exception);
			}
		});
	}

	public void forceAndClose(CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				channel.force(true);
				channel.close();
			}
		}, callback);
	}

	/**
	 * Closes the channel
	 *
	 * @param callback which will be called after complete
	 */
	@Override
	public void close(CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				channel.close();
			}
		}, callback);
	}

	/**
	 * Truncates this file to the given size.
	 *
	 * @param size     the new size, a non-negative byte count
	 * @param callback which will be called after complete
	 */
	@Override
	public void truncate(final long size, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				channel.truncate(size);
			}
		}, callback);
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it.
	 *
	 * @param metaData if true then this method is required to force changes to both the file's
	 *                 content and metadata to be written to storage; otherwise, it need only force content changes to be written
	 * @param callback which will be called after complete
	 */
	@Override
	public void force(final boolean metaData, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				channel.force(metaData);
			}
		}, callback);
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
