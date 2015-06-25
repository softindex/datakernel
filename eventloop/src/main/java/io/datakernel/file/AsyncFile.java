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

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.async.AsyncCallbacks.*;

/**
 * An abstract representation of file. Actions with this file are non-blocking
 */
public final class AsyncFile implements File {
	private final Eventloop eventloop;
	private final ExecutorService executor;

	private final AsynchronousFileChannel channel;

	/**
	 * Creates a new instance of AsyncFile
	 *
	 * @param eventloop event loop in which will be using file
	 * @param executor  executor for running tasks in other thread
	 * @param channel   an asynchronous channel for reading, writing, and manipulating a file.
	 */
	private AsyncFile(Eventloop eventloop, ExecutorService executor,
	                  AsynchronousFileChannel channel) {
		this.eventloop = checkNotNull(eventloop);
		this.executor = checkNotNull(executor);
		this.channel = checkNotNull(channel);
	}

	/**
	 * Asynchronous opens file
	 *
	 * @param eventloop   event loop in which will be using file
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
				return (T) new AsyncFile(eventloop, executor, channel);
			}
		}, callback);
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
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						callback.onResult(result);
					}
				});
				tracker.complete();
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
				tracker.complete();
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
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						callback.onResult(result);
					}
				});
				tracker.complete();
			}

			@Override
			public void failed(final Throwable exc, Object attachment) {
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
				tracker.complete();
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
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onComplete();
						}
					});
					tracker.complete();
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
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
				tracker.complete();
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
					eventloop.postConcurrently(new Runnable() {
						@Override
						public void run() {
							callback.onComplete();
						}
					});
					tracker.complete();
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
				eventloop.postConcurrently(new Runnable() {
					@Override
					public void run() {
						callback.onException(exc instanceof Exception ? (Exception) exc : new Exception(exc));
					}
				});
				tracker.complete();
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

		final ByteBuf buf = eventloop.getByteBufferPool().allocate((int) size);
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
}
