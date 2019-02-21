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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.CloseException;
import io.datakernel.exception.UncheckedException;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.Executor;

import static io.datakernel.async.Promise.ofBlockingRunnable;
import static io.datakernel.util.CollectionUtils.set;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Recyclable.tryRecycle;
import static java.nio.file.StandardOpenOption.*;

/**
 * Represents a file with asynchronous capabilities.
 * Utilizes {@link FileChannel} for file I/O operations.
 */
public final class AsyncFile {
	public static final CloseException FILE_CLOSED = new CloseException(AsyncFile.class, "File has been closed");
	private final Executor executor;
	private final FileChannel channel;

	private final Path path;
	private final Object mutexLock;

	private AsyncFile(Executor executor, FileChannel channel, Path path, @Nullable Object mutexLock) {
		this.executor = checkNotNull(executor);
		this.channel = checkNotNull(channel);
		this.path = checkNotNull(path);
		this.mutexLock = mutexLock != null ? mutexLock : this;
	}

	/**
	 * Opens file synchronously.
	 *
	 * @param executor    executor for running tasks in other thread
	 * @param path        the path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static AsyncFile open(Executor executor, Path path, Set<OpenOption> openOptions) throws IOException {
		FileChannel channel = doOpenChannel(path, openOptions);
		return new AsyncFile(executor, channel, path, null);
	}

	public static AsyncFile open(Executor executor, Path path, Set<OpenOption> openOptions, Object mutexLock) throws IOException {
		FileChannel channel = doOpenChannel(path, openOptions);
		return new AsyncFile(executor, channel, path, mutexLock);
	}

	/**
	 * Opens file asynchronously.
	 *
	 * @param executor    executor for running tasks in other thread
	 * @param path        the path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 */
	public static Promise<AsyncFile> openAsync(Executor executor, Path path, Set<OpenOption> openOptions) {
		return Promise.ofBlockingCallable(executor, () -> doOpenChannel(path, openOptions))
				.thenApply(channel -> new AsyncFile(executor, channel, path, null));
	}

	public static Promise<AsyncFile> openAsync(Executor executor, Path path, Set<OpenOption> openOptions, Object mutexLock) {
		return Promise.ofBlockingCallable(executor, () -> doOpenChannel(path, openOptions))
				.thenApply(channel -> new AsyncFile(executor, channel, path, mutexLock));
	}

	private static FileChannel doOpenChannel(Path path, Set<OpenOption> openOptions) throws IOException {
		return FileChannel.open(path, openOptions);
	}

	/**
	 * Deletes the file concurrently.
	 *
	 * @param path the path of the file to open or create
	 */
	public static Promise<Void> delete(Executor executor, Path path) {
		return ofBlockingRunnable(executor, () -> {
			try {
				Files.delete(path);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Checks the file size. For non-regular or non-existing files returns <code>null</code>.
	 *
	 * @param executor executor for running tasks in another thread
	 * @param path     the path of the file to check
	 * @return file size if given path is a regular file and <code>null</code> if it is a directory or it does not exist
	 */
	public static Promise<Long> size(Executor executor, Path path) {
		return Promise.ofBlockingCallable(executor, () -> Files.isRegularFile(path) ? Files.size(path) : null);
	}

	/**
	 * Moves or renames a file to a target file.
	 *
	 * @param executor executor for running tasks in another thread
	 * @param source   the path to the file to move
	 * @param target   the path to the target file (may be associated with a different provider to the source path)
	 * @param options  options specifying how the move should be done
	 */
	public static Promise<Void> move(Executor executor, Path source, Path target, CopyOption... options) {
		return ofBlockingRunnable(executor, () -> {
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
	 * @param executor executor for running tasks in  another thread
	 * @param source   the path to the file to copy
	 * @param target   the path to the target file (may be associated with a different provider to the source path)
	 * @param options  options specifying how the move should be done
	 */
	public static Promise<Void> copy(Executor executor, Path source, Path target, CopyOption... options) {
		return ofBlockingRunnable(executor, () -> {
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
	public static Promise<Void> createDirectory(Executor executor, Path dir, @Nullable FileAttribute<?>[] attrs) {
		return ofBlockingRunnable(executor, () -> {
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
	public static Promise<Void> createDirectories(Executor executor, Path dir, FileAttribute... attrs) {
		return ofBlockingRunnable(executor, () -> {
			try {
				Files.createDirectories(dir, attrs);
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
	public static Promise<ByteBuf> readFile(Executor executor, Path path) {
		return openAsync(executor, path, set(READ))
				.thenCompose(file -> file.read()
						.thenCompose(buf -> file.close()
								.whenException($ -> buf.recycle())
								.thenApply($ -> buf)));
	}

	/**
	 * Creates new file and writes a sequence of bytes to this file from
	 * the given buffer, starting at the given file position.
	 * If file already exists, promise fails with exception.
	 *
	 * @param path the path of the file to create and write
	 * @param buf  the buffer from which bytes are to be transferred byteBuffer
	 */
	public static Promise<Void> writeNewFile(Executor executor, Path path, ByteBuf buf) {
		return openAsync(executor, path, set(WRITE, CREATE_NEW))
				.thenCompose(file -> file.write(buf)
						.thenCompose($ -> file.close()))
				.whenException($ -> buf.recycle());
	}

	public Executor getExecutor() {
		return executor;
	}

	public Path getPath() {
		return path;
	}

	public FileChannel getChannel() {
		return channel;
	}

	public Promise<Long> size() {
		return sanitize(AsyncFile.size(executor, path));
	}

	public Promise<Void> seek(long position) {
		return sanitize(ofBlockingRunnable(executor, () -> {
			try {
				channel.position(position);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		}));
	}

	public Promise<Long> tell() {
		return sanitize(Promise.ofBlockingCallable(executor, channel::position));
	}

	/**
	 * Writes all bytes of the buffer into this file
	 * at its internal position asynchronously.
	 *
	 * @param buf byte buffer to be written
	 */
	public Promise<Void> write(ByteBuf buf) {
		return sanitize(ofBlockingRunnable(executor, () -> {
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
	 * Writes all bytes of the buffer into this file
	 * starting at given position asynchronously.
	 *
	 * @param position offset from which bytes will be written to the file
	 * @param buf      byte buffer to be written
	 */
	public Promise<Void> write(ByteBuf buf, long position) {
		return sanitize(ofBlockingRunnable(executor, () -> {
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
	 * Reads all bytes from this file into a buffer asynchronously.
	 */
	public Promise<ByteBuf> read() {
		return read(0);
	}

	/**
	 * Reads all bytes from this file into a buffer asynchronously,
	 * starting at the given position.
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
	 * Reads a sequence of bytes from this channel into the given buffer asynchronously,
	 * starting at the given position.
	 * Reads are happening in other thread(s).
	 *
	 * @param buf      the buffer into which bytes are to be transferred
	 * @param position the file position at which the transfer is to begin;
	 *                 must be non-negative
	 */
	public Promise<Void> read(ByteBuf buf, long position) {
		return sanitize(ofBlockingRunnable(executor, () -> {
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
	 * if the file is opened.
	 *
	 * @param forceMetadata whether or not to force metadata writes too
	 */
	public Promise<Void> forceAndClose(boolean forceMetadata) {
		if (!isOpen()) return Promise.ofException(FILE_CLOSED);
		return ofBlockingRunnable(executor, () -> {
			try {
				channel.force(forceMetadata);
				channel.close();
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Closes the channel.
	 */
	public Promise<Void> close() {
		if (!isOpen()) return Promise.ofException(FILE_CLOSED);
		return ofBlockingRunnable(executor, () -> {
			try {
				channel.close();
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		});
	}

	/**
	 * Truncates this file to the given size. If current size
	 * of the file is greater than {@code size}, all bytes
	 * beyond the new size will be discarded.
	 *
	 * @param size the new size, a non-negative byte count
	 */
	public Promise<Void> truncate(long size) {
		return sanitize(ofBlockingRunnable(executor, () -> {
			try {
				channel.truncate(size);
			} catch (IOException e) {
				throw new UncheckedException(e);
			}
		}));
	}

	/**
	 * Forces any updates to this file to be written
	 * to the storage device that contains it.
	 *
	 * @param metaData if {@code true}, this method is required
	 *                 to force changes of both file content and
	 *                 metadata to be written to storage; otherwise, it
	 *                 only needs to force content changes to be written
	 */
	public Promise<Void> force(boolean metaData) {
		return sanitize(ofBlockingRunnable(executor, () -> {
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
		return promise.thenComposeEx((result, e) -> {
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
