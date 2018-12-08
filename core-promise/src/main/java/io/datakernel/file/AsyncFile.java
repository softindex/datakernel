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

package io.datakernel.file;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.CloseException;
import io.datakernel.exception.UncheckedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.Promise.ofRunnable;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Recyclable.tryRecycle;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Arrays.asList;

/**
 * This class represents a file with asynchronous capabilities.
 */
public final class AsyncFile {
	public static final CloseException FILE_CLOSED = new CloseException(AsyncFile.class, "File has been closed");
	private final ExecutorService executor;
	private final FileChannel channel;

	private final Path path;
	private final Object mutexLock;

	private AsyncFile(ExecutorService executor, FileChannel channel, Path path, @Nullable Object mutexLock) {
		this.executor = checkNotNull(executor);
		this.channel = checkNotNull(channel);
		this.path = checkNotNull(path);
		this.mutexLock = mutexLock != null ? mutexLock : this;
	}

	/**
	 * Synchronously opens file
	 *
	 * @param executor    executor for running tasks in other thread
	 * @param path        the path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static AsyncFile open(ExecutorService executor, Path path, OpenOption[] openOptions) throws IOException {
		FileChannel channel = doOpenChannel(path, openOptions);
		return new AsyncFile(executor, channel, path, null);
	}

	public static AsyncFile open(ExecutorService executor, Path path, OpenOption[] openOptions, Object mutexLock) throws IOException {
		FileChannel channel = doOpenChannel(path, openOptions);
		return new AsyncFile(executor, channel, path, mutexLock);
	}

	/**
	 * Asynchronously opens file
	 *
	 * @param executor    executor for running tasks in other thread
	 * @param path        the path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static Promise<AsyncFile> openAsync(ExecutorService executor, Path path, OpenOption[] openOptions) {
		return Promise.ofCallable(executor, () -> doOpenChannel(path, openOptions))
				.thenApply(channel -> new AsyncFile(executor, channel, path, null));
	}

	public static Promise<AsyncFile> openAsync(ExecutorService executor, Path path, OpenOption[] openOptions, Object mutexLock) {
		return Promise.ofCallable(executor, () -> doOpenChannel(path, openOptions))
				.thenApply(channel -> new AsyncFile(executor, channel, path, mutexLock));
	}

	private static FileChannel doOpenChannel(Path path, OpenOption[] openOptions) throws IOException {
		return FileChannel.open(path, new HashSet<>(asList(openOptions)));
	}

	/**
	 * Concurrently deletes the file.
	 *
	 * @param path the path of the file to open or create
	 */
	public static Promise<Void> delete(ExecutorService executor, Path path) {
		return ofRunnable(executor, () -> {
			try {
				Files.delete(path);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Checks the file size. For non-regular or non-existing files returns <code>null</code>
	 *
	 * @param executor executor for running tasks in other thread
	 * @param path     the path of the file to check
	 * @return file size if given path is a regular file and <code>null</code> if it is a directory or it does not exist
	 */
	public static Promise<Long> size(ExecutorService executor, Path path) {
		return Promise.ofCallable(executor, () -> Files.isRegularFile(path) ? Files.size(path) : null);
	}

	/**
	 * Moves or renames a file to a target file.
	 *
	 * @param executor executor for running tasks in other thread
	 * @param source   the path to the file to move
	 * @param target   the path to the target file (may be associated with a different provider to the source path)
	 * @param options  options specifying how the move should be done
	 */
	public static Promise<Void> move(ExecutorService executor, Path source, Path target, CopyOption... options) {
		return ofRunnable(executor, () -> {
			try {
				Files.move(source, target, options);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Copies a file to a target file.
	 *
	 * @param executor executor for running tasks in other thread
	 * @param source   the path to the file to copy
	 * @param target   the path to the target file (may be associated with a different provider to the source path)
	 * @param options  options specifying how the move should be done
	 */
	public static Promise<Void> copy(ExecutorService executor, Path source, Path target, CopyOption... options) {
		return ofRunnable(executor, () -> {
			try {
				Files.copy(source, target, options);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Creates a new directory.
	 *
	 * @param executor executor for running tasks in other thread
	 * @param dir      the directory to create
	 * @param attrs    an optional list of file attributes to set atomically when creating the directory
	 */
	public static Promise<Void> createDirectory(ExecutorService executor, Path dir, @Nullable FileAttribute<?>[] attrs) {
		return ofRunnable(executor, () -> {
			try {
				Files.createDirectory(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Creates a directory by creating all nonexistent parent directories first.
	 *
	 * @param executor executor for running tasks in other thread
	 * @param dir      the directory to create
	 * @param attrs    an optional list of file attributes to set atomically when creating the directory
	 */
	public static Promise<Void> createDirectories(ExecutorService executor, Path dir, @Nullable FileAttribute<?>[] attrs) {
		return ofRunnable(executor, () -> {
			try {
				Files.createDirectories(dir, attrs == null ? new FileAttribute<?>[0] : attrs);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Reads all bytes from this channel into the given buffer.
	 *
	 * @param path the path of the file to read
	 */
	public static Promise<ByteBuf> readFile(ExecutorService executor, Path path) {
		return openAsync(executor, path, new OpenOption[]{READ})
				.thenCompose(file -> file.read()
						.thenCompose(buf -> file.close()
								.whenException($ -> buf.recycle())
								.thenApply($ -> buf)));
	}

	/**
	 * Creates new file and writes a sequence of bytes to this file from the given buffer, starting at the given file
	 * position.
	 * If file exists then promise fails with exception.
	 *
	 * @param path the path of the file to create and write
	 * @param buf  the buffer from which bytes are to be transferred byteBuffer
	 */
	public static Promise<Void> writeNewFile(ExecutorService executor, Path path, ByteBuf buf) {
		return openAsync(executor, path, new OpenOption[]{WRITE, CREATE_NEW})
				.thenCompose(file -> file.write(buf)
						.thenCompose($ -> file.close()
								.whenException($2 -> buf.recycle())));
	}

	public ExecutorService getExecutor() {
		return executor;
	}

	public FileChannel getChannel() {
		return channel;
	}

	public Promise<Long> size() {
		return sanitize(AsyncFile.size(executor, path));
	}

	public Promise<Void> seek(long position) {
		return sanitize(ofRunnable(executor, () -> {
			try {
				channel.position(position);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		}));
	}

	public Promise<Long> tell() {
		return sanitize(Promise.ofCallable(executor, channel::position));
	}

	/**
	 * Asynchronously writes all bytes of the buffer into this file at its internal position.
	 *
	 * @param buf byte buffer to be written
	 */
	public Promise<Void> write(ByteBuf buf) {
		return sanitize(ofRunnable(executor, () -> {
			synchronized (mutexLock) {
				try {
					int writtenBytes;
					do {
						ByteBuffer byteBuffer = buf.toReadByteBuffer();
						writtenBytes = channel.write(byteBuffer);
						buf.ofReadByteBuffer(byteBuffer);
					} while (writtenBytes != -1 && buf.canRead());
				} catch (IOException e) {
					throw new UncheckedException(e);
				} finally {
					buf.recycle();
				}
			}
		}));
	}

	/**
	 * Asynchronously writes all bytes of the buffer into this file starting at given position.
	 *
	 * @param position offset from which bytes will be written to the file
	 * @param buf      byte buffer to be written
	 */
	public Promise<Void> write(ByteBuf buf, long position) {
		return sanitize(ofRunnable(executor, () -> {
			synchronized (mutexLock) {
				int writtenBytes = 0;
				long pos = position;
				try {
					do {
						ByteBuffer byteBuffer = buf.toReadByteBuffer();
						writtenBytes = channel.write(byteBuffer, pos += writtenBytes);
						buf.ofReadByteBuffer(byteBuffer);
					} while (writtenBytes != -1 && buf.canRead());
				} catch (IOException e) {
					throw new UncheckedException(e);
				} finally {
					buf.recycle();
				}
			}
		}));
	}

	/**
	 * Asynchronously reads all bytes from this file into a buffer.
	 */
	public Promise<ByteBuf> read() {
		return read(0);
	}

	/**
	 * Asynchronously reads all bytes from this file into a buffer starting at given position.
	 *
	 * @param position offset from which bytes of the file will be read
	 */
	public Promise<ByteBuf> read(long position) {
		long size;

		try {
			size = channel.size();
		} catch (IOException e) {
			return Promise.ofException(e);
		}

		ByteBuf buf = ByteBufPool.allocate((int) (size - position));
		return read(buf, position)
				.whenException($ -> buf.recycle())
				.thenApply($ -> buf);
	}

	/**
	 * Asynchronously reads a sequence of bytes from this channel into the given buffer,
	 * starting at the given position.
	 * Reads are happenning in other thread(s).
	 *
	 * @param buf      the buffer into which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin; must be non-negative
	 */
	public Promise<Void> read(ByteBuf buf, long position) {
		return sanitize(ofRunnable(executor, () -> {
			synchronized (mutexLock) {
				int readBytes = 0;
				long pos = position;
				do {
					ByteBuffer byteBuffer = buf.toWriteByteBuffer();
					try {
						readBytes = channel.read(byteBuffer, pos += readBytes);
					} catch (IOException e) {
						throw new UncheckedException(e);
					}
					buf.ofWriteByteBuffer(byteBuffer);
				} while (readBytes != -1 && buf.canWrite());
			}
		}));
	}

	/**
	 * Forces physical write and then closes the channel
	 *
	 * @param forceMetadata whether or not to force metadata writes too
	 */
	public Promise<Void> forceAndClose(boolean forceMetadata) {
		if (!isOpen()) return Promise.ofException(FILE_CLOSED);
		return ofRunnable(executor, () -> {
			try {
				channel.force(forceMetadata);
				channel.close();
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Closes the channel
	 */
	public Promise<Void> close() {
		if (!isOpen()) return Promise.ofException(FILE_CLOSED);
		return ofRunnable(executor, () -> {
			try {
				channel.close();
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Truncates this file to the given size.
	 *
	 * @param size the new size, a non-negative byte count
	 */
	public Promise<Void> truncate(long size) {
		return sanitize(ofRunnable(executor, () -> {
			try {
				channel.truncate(size);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		}));
	}

	/**
	 * Forces any updates to this file to be written to the storage device that contains it.
	 *
	 * @param metaData if true then this method is required to force changes of both
	 *                 file content and metadata to be written to storage;
	 *                 otherwise, it need only force content changes to be written
	 */
	public Promise<Void> force(boolean metaData) {
		return sanitize(ofRunnable(executor, () -> {
			try {
				channel.force(metaData);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		}));
	}

	public boolean isOpen() {
		return channel.isOpen();
	}

	private <T> Promise<T> sanitize(Promise<T> promise) {
		return promise
				.thenComposeEx((result, e) -> {
					if (!isOpen()) {
						tryRecycle(result);
						return Promise.ofException(FILE_CLOSED);
					}
					return Promise.of(result, e);
				});
	}

	@Override
	public String toString() {
		return path.toString();
	}
}
