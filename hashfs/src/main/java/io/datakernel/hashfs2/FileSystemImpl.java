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

package io.datakernel.hashfs2;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class FileSystemImpl implements FileSystem {
	private static final String IN_PROGRESS_EXTENSION = ".partial";
	private static final String TMP_DIRECTORY = "tmp";

	private final ExecutorService executor;
	private final NioEventloop eventloop;

	private final Path fileStorage;
	private final Path tmpStorage;
	private final int bufferSize;

	private FileSystemImpl(NioEventloop eventloop, ExecutorService executor,
	                       Path fileStorage, Path tmpStorage, int bufferSize) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.fileStorage = fileStorage;
		this.tmpStorage = tmpStorage;
		this.bufferSize = bufferSize;
	}

	public static FileSystem init(NioEventloop eventloop, ExecutorService executor,
	                              Path fileStorage, int bufferSize) throws IOException {
		Path tmpStorage = fileStorage.resolve(TMP_DIRECTORY);
		FileSystemImpl fileSystem = new FileSystemImpl(eventloop, executor, fileStorage, tmpStorage, bufferSize);
		fileSystem.ensureInfrastructure();
		fileSystem.cleanFolder(tmpStorage);
		return fileSystem;
	}

	@Override
	public void saveToTemporary(String filePath, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		Path tmpPath;
		try {
			tmpPath = ensureInProgressDirectory(filePath);
		} catch (IOException e) {
			callback.onException(e);
			return;
		}
		StreamFileWriter diskWrite = StreamFileWriter.createFile(eventloop, executor, tmpPath, true);
		diskWrite.setFlushCallback(callback);
		producer.streamTo(diskWrite);
	}

	@Override
	public void commitTemporary(String filePath, boolean successful, CompletionCallback callback) {
		if (successful) {
			Path destinationPath;
			Path tmpPath;
			try {
				tmpPath = ensureInProgressDirectory(filePath);
				destinationPath = ensureDestinationDirectory(filePath);
			} catch (IOException e) {
				callback.onException(e);
				return;
			}
			try {
				Files.move(tmpPath, destinationPath);
				callback.onComplete();
			} catch (IOException e) {
				try {
					Files.delete(tmpPath);
				} catch (IOException ignored) {

				}
				callback.onException(e);
			}
		} else {
			deleteTemporary(filePath, callback);
		}
	}

	@Override
	public void get(String filePath, StreamConsumer<ByteBuf> consumer) {
		Path destination = fileStorage.resolve(filePath);
		StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, bufferSize, destination);
		producer.streamTo(consumer);
	}

	@Override
	public void deleteFile(String filePath, CompletionCallback callback) {
		Path path = fileStorage.resolve(filePath);
		try {
			deleteFile(path);
			callback.onComplete();
		} catch (IOException e) {
			callback.onException(e);
		}
	}

	@Override
	public void listFiles(ResultCallback<Set<String>> files) {
		Set<String> result = new HashSet<>();
		try {
			listFiles(fileStorage, result);
			files.onResult(result);
		} catch (IOException e) {
			files.onException(e);
		}
	}

	private void listFiles(Path parent, Set<String> files) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(parent)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					if (!path.equals(tmpStorage)) {
						listFiles(path, files);
					}
				} else {
					files.add(path.getFileName().toString());
				}
			}
		}
	}

	private Path ensureDestinationDirectory(String path) throws IOException {
		return ensureDirectory(fileStorage, path);
	}

	private Path ensureInProgressDirectory(String path) throws IOException {
		return ensureDirectory(tmpStorage, path + IN_PROGRESS_EXTENSION);
	}

	private Path ensureDirectory(Path container, String path) throws IOException {
		String fileName = getFileName(path);
		String filePath = getFilePath(path);

		Path destinationDirectory = container.resolve(filePath);

		Files.createDirectories(destinationDirectory);

		return destinationDirectory.resolve(fileName);
	}

	private String getFileName(String path) {
		if (path.contains(File.separator)) {
			path = path.substring(path.lastIndexOf(File.separator) + 1);
		}
		return path;
	}

	private String getFilePath(String path) {
		if (path.contains(File.separator)) {
			path = path.substring(0, path.lastIndexOf(File.separator));
		} else {
			path = "";
		}
		return path;
	}

	private void deleteTemporary(String filePath, CompletionCallback callback) {
		Path path = tmpStorage.resolve(filePath + IN_PROGRESS_EXTENSION);
		try {
			deleteFile(path);
			callback.onComplete();
		} catch (IOException e) {
			callback.onException(e);
		}
	}

	private void deleteFile(Path path) throws IOException {
		if (Files.isDirectory(path)) {
			if (isDirEmpty(path)) {
				Files.delete(path);
			}
			path = path.getParent();
			if (path != null && !path.equals(fileStorage)) {
				deleteFile(path);
			}
		} else {
			Files.delete(path);
			deleteFile(path.getParent());
		}
	}

	private boolean isDirEmpty(final Path directory) throws IOException {
		try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
			return !dirStream.iterator().hasNext();
		}
	}

	private void ensureInfrastructure() throws IOException {
		Files.createDirectory(tmpStorage);
	}

	private void cleanFolder(Path container) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(container)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path) && path.iterator().hasNext()) {
					cleanFolder(path);
				} else {
					Files.delete(path);
				}
			}
		}
	}
}
