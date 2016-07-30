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

package io.datakernel;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static io.datakernel.stream.file.StreamFileReader.readFileFrom;
import static io.datakernel.stream.file.StreamFileWriter.create;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.*;

public final class FileManager {
	private static final Logger logger = LoggerFactory.getLogger(FileManager.class);

	private static final OpenOption[] CREATE_OPTIONS = new OpenOption[]{WRITE, CREATE_NEW};

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Path storagePath;

	private int readerBufferSize = 256 * (1 << 10);     // 256kb

	public FileManager(Eventloop eventloop, ExecutorService executor, Path storagePath) {
		this.eventloop = checkNotNull(eventloop);
		this.executor = checkNotNull(executor);
		this.storagePath = checkNotNull(storagePath);

		if (Files.notExists(storagePath)) {
			try {
				Files.createDirectories(storagePath);
			} catch (IOException ignore) {
				throw new AssertionError("Can't create storage directory");
			}
		}
	}

	public void get(String fileName, final long startPosition, final ResultCallback<StreamFileReader> callback) {
		logger.trace("downloading file: {}, position: {}", fileName, startPosition);
		AsyncFile.open(eventloop, executor, storagePath.resolve(fileName),
				new OpenOption[]{READ}, new ForwardingResultCallback<AsyncFile>(callback) {
					@Override
					public void onResult(AsyncFile result) {
						logger.trace("{} opened", result);
						callback.onResult(readFileFrom(eventloop, result, readerBufferSize, startPosition));
					}
				});
	}

	public void save(String fileName, final ResultCallback<StreamFileWriter> callback) {
		logger.trace("uploading file: {}", fileName);
		ensureDirectory(storagePath, fileName, new ForwardingResultCallback<Path>(callback) {
			@Override
			public void onResult(Path path) {
				logger.trace("ensured directory: {}", storagePath);
				AsyncFile.open(eventloop, executor, path, CREATE_OPTIONS, new ForwardingResultCallback<AsyncFile>(callback) {
					@Override
					public void onResult(AsyncFile result) {
						logger.trace("{} opened", result);
						callback.onResult(create(eventloop, result, true));
					}
				});
			}
		});
	}

	public void delete(String fileName, CompletionCallback callback) {
		logger.trace("deleting file: {}", fileName);
		AsyncFile.delete(eventloop, executor, storagePath.resolve(fileName), callback);
	}

	public void size(String fileName, ResultCallback<Long> callback) {
		logger.trace("calculating file size: {}", fileName);
		AsyncFile.length(eventloop, executor, storagePath.resolve(fileName), callback);
	}

	public void scan(ResultCallback<List<String>> callback) {
		eventloop.callConcurrently(executor, new Callable<List<String>>() {
			@Override
			public List<String> call() throws Exception {
				logger.trace("listing files");
				List<String> result = new ArrayList<>();
				doScan(storagePath, result, "");
				return result;
			}
		}, callback);
	}

	public List<String> scan() throws IOException {
		List<String> result = new ArrayList<>();
		doScan(storagePath, result, "");
		return result;
	}

	private void doScan(Path parent, List<String> files, String pathFromRoot) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(parent)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					doScan(path, files, pathFromRoot + path.getFileName().toString() + File.separator);
				} else {
					files.add(pathFromRoot + path.getFileName().toString());
				}
			}
		}
	}

	private void ensureDirectory(final Path container, final String path, ResultCallback<Path> callback) {
		eventloop.callConcurrently(executor, new Callable<Path>() {
			@Override
			public Path call() throws Exception {
				return ensureDirectory(container, path);
			}
		}, callback);
	}

	private Path ensureDirectory(Path container, String path) throws IOException {
		String file;
		String filePath;

		if (path.contains(File.separator)) {
			int index = path.lastIndexOf(File.separator);
			file = path.substring(index + 1);
			filePath = path.substring(0, index);
		} else {
			file = path;
			filePath = "";
		}

		Path destinationDirectory = container.resolve(filePath);
		Files.createDirectories(destinationDirectory);
		return destinationDirectory.resolve(file);
	}
}
