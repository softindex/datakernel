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
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.RunnableWithException;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;

import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.async.AsyncCallbacks.runConcurrently;
import static java.nio.file.StandardOpenOption.*;

/**
 * Factory methods for file systems.
 */
public final class AsyncFileSystem implements FileSystem {
	private final Eventloop eventloop;
	private final ExecutorService executor;

	/**
	 * Creates new instance of AsyncFileSystem
	 *
	 * @param eventloop event loop in which will be using file
	 * @param executor  executor for running tasks in other thread
	 */
	public AsyncFileSystem(Eventloop eventloop, ExecutorService executor) {
		this.eventloop = eventloop;
		this.executor = executor;
	}

	/**
	 * Asynchronous opens file
	 *
	 * @param path        the  path of the file to open or create
	 * @param openOptions options specifying how the file is opened
	 * @param callback    callback which will be called after opening
	 */
	@Override
	public <T extends File> void open(Path path, OpenOption[] openOptions, ResultCallback<T> callback) {
		AsyncFile.open(eventloop, executor, path, openOptions, callback);
	}

	/**
	 * Deletes the file in new thread
	 *
	 * @param path     the  path of the file to open or create
	 * @param callback callback which will be called after opening
	 */
	@Override
	public void delete(final Path path, CompletionCallback callback) {
		runConcurrently(eventloop, executor, false, new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				Files.delete(path);
			}
		}, callback);
	}

	/**
	 * Moves or renames a file to a target file.
	 *
	 * @param source   the path to the file to move
	 * @param target   the path to the target file (may be associated with a different provider to the source path)
	 * @param options  options specifying how the move should be done
	 * @param callback callback which will be called after moving
	 */
	@Override
	public void move(final Path source, final Path target, final CopyOption[] options, CompletionCallback callback) {
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
	 * @param dir      the directory to create
	 * @param attrs    an optional list of file attributes to set atomically when creating the directory
	 * @param callback callback which will be called after creating
	 */
	@Override
	public void createDirectory(final Path dir, @Nullable final FileAttribute<?>[] attrs, CompletionCallback callback) {
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
	 * @param dir      the directory to create
	 * @param attrs    an optional list of file attributes to set atomically when creating the directory
	 * @param callback callback which will be called after creating
	 */
	@Override
	public void createDirectories(final Path dir, @Nullable final FileAttribute<?>[] attrs, CompletionCallback callback) {
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
	 * @param path     the  path of the file to read
	 * @param callback which will be called after complete
	 */
	@Override
	public void readFile(Path path, final ResultCallback<ByteBuf> callback) {
		open(path, new OpenOption[]{READ}, new ForwardingResultCallback<AsyncFile>(callback) {
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
	 * @param path     the  path of the file to create and write
	 * @param buf      the  buffer from which bytes are to be transferred byteBuffer
	 * @param callback which will be called after complete
	 */
	@Override
	public void createNewAndWriteFile(Path path, final ByteBuf buf, final CompletionCallback callback) {
		open(path, new OpenOption[]{WRITE, CREATE_NEW}, new ForwardingResultCallback<AsyncFile>(callback) {
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

}
