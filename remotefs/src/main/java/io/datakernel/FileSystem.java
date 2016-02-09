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

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import org.slf4j.Logger;

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

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.*;
import static org.slf4j.LoggerFactory.getLogger;

public final class FileSystem {
	private static final Logger logger = getLogger(FileSystem.class);

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Path storage;

	// creators
	private FileSystem(Eventloop eventloop, ExecutorService executor, Path storage) {
		this.eventloop = checkNotNull(eventloop);
		this.executor = checkNotNull(executor);
		this.storage = checkNotNull(storage);
	}

	public static FileSystem newInstance(Eventloop eventloop, ExecutorService executor, Path storage) {
		return new FileSystem(eventloop, executor, storage);
	}

	// api
	public void save(String fileName, final ResultCallback<AsyncFile> callback) {
		logger.trace("Opening file {}", fileName);
		ensureDirectory(storage, fileName, new ResultCallback<Path>() {
			@Override
			public void onResult(Path path) {
				AsyncFile.open(eventloop, executor, path, new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING}, callback);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Caught exception while trying to ensure directory", e);
				callback.onException(e);
			}
		});
	}

	public void get(String fileName, ResultCallback<AsyncFile> callback) {
		logger.trace("Streaming file {}", fileName);
		Path destination = storage.resolve(fileName);
		AsyncFile.open(eventloop, executor, destination, new OpenOption[]{READ}, callback);
	}

	public void delete(String fileName, CompletionCallback callback) {
		logger.trace("Deleting file {}", fileName);
		Path path = storage.resolve(fileName);
		AsyncFile.delete(eventloop, executor, path, callback);
	}

	public void list(ResultCallback<List<String>> callback) {
		logger.trace("Listing files");
		listFiles(storage, callback);
	}

	public void fileSize(String fileName, ResultCallback<Long> callback) {
		Path filePath = storage.resolve(fileName);
		AsyncFile.length(eventloop, executor, filePath, callback);
	}

	public void initDirectories() throws IOException {
		Files.createDirectories(storage);
	}

	// utils
	private void listFiles(final Path parent, ResultCallback<List<String>> callback) {
		AsyncCallbacks.callConcurrently(eventloop, executor, false, new Callable<List<String>>() {
			@Override
			public List<String> call() throws Exception {
				List<String> result = new ArrayList<>();
				listFiles(parent, result, "");
				return result;
			}
		}, callback);
	}

	private void listFiles(Path parent, List<String> files, String previousPath) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(parent)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					listFiles(path, files, previousPath + path.getFileName().toString() + File.separator);
				} else {
					files.add(previousPath + path.getFileName().toString());
				}
			}
		}
	}

	private void ensureDirectory(final Path container, final String path, ResultCallback<Path> callback) {
		AsyncCallbacks.callConcurrently(eventloop, executor, false, new Callable<Path>() {
			@Override
			public Path call() throws Exception {
				return ensureDirectory(container, path);
			}
		}, callback);
	}

	private Path ensureDirectory(Path container, String path) throws IOException {
		String fileName = getFileName(path);
		String filePath = getPathToFile(path);
		Path destinationDirectory = container.resolve(filePath);
		Files.createDirectories(destinationDirectory);
		return destinationDirectory.resolve(fileName);
	}

	private String getFileName(String filePath) {
		if (filePath.contains(File.separator)) {
			filePath = filePath.substring(filePath.lastIndexOf(File.separator) + 1);
		}
		return filePath;
	}

	private String getPathToFile(String filePath) {
		if (filePath.contains(File.separator)) {
			filePath = filePath.substring(0, filePath.lastIndexOf(File.separator));
		} else {
			filePath = "";
		}
		return filePath;
	}
}