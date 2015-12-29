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

package io.datakernel.remotefs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public final class FileSystemImpl implements FileSystem {
	private static final Logger logger = LoggerFactory.getLogger(FileSystemImpl.class);
	private final String inProgressExtension;
	private final int bufferSize;

	private final ExecutorService executor;
	private final NioEventloop eventloop;

	private final Path fileStorage;
	private final Path tmpStorage;

	private FileSystemImpl(NioEventloop eventloop, ExecutorService executor,
	                       Path fileStorage, Path tmpStorage, int bufferSize,
	                       String inProgressExtension) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.fileStorage = fileStorage;
		this.tmpStorage = tmpStorage;
		this.bufferSize = bufferSize;
		this.inProgressExtension = inProgressExtension;
	}

	public static FileSystem createInstance(NioEventloop eventloop, ExecutorService executor, Path storage, RfsConfig config) {
		Path tmpStorage = storage.resolve(config.getTmpDirectoryName());
		return new FileSystemImpl(eventloop, executor, storage, tmpStorage,
				config.getFsReaderBufferSize(), config.getInProgressExtension());
	}

	@Override
	public NioEventloop getNioEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		try {
			this.ensureInfrastructure();
			logger.trace("FileSystem for HashFs initiated");
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't initiate FileSystem for HashFs", e);
			callback.onException(e);
		}
	}

	@Override
	public void stop(CompletionCallback callback) {
		logger.trace("FileSystem stopped");
		callback.onComplete();
	}

	@Override
	public void saveToTmp(String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		logger.trace("Saving to temporary dir {}", fileName);
		Path tmpPath;
		try {
			tmpPath = ensureInProgressDirectory(fileName);
		} catch (IOException e) {
			callback.onException(e);
			return;
		}
		StreamFileWriter diskWrite = StreamFileWriter.createFile(eventloop, executor, tmpPath, true, true);
		diskWrite.setFlushCallback(callback);
		producer.streamTo(diskWrite);
	}

	@Override
	public void commitTmp(String fileName, CompletionCallback callback) {
		logger.trace("Moving file from temporary dir to {}", fileName);
		Path destinationPath;
		Path tmpPath;
		try {
			tmpPath = ensureInProgressDirectory(fileName);
			destinationPath = ensureDestinationDirectory(fileName);
		} catch (IOException e) {
			logger.error("Can't ensure directory for {}", fileName, e);
			callback.onException(e);
			return;
		}
		try {
			Files.move(tmpPath, destinationPath);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't move file from temporary dir to {}", fileName, e);
			try {
				Files.delete(tmpPath);
			} catch (IOException ignored) {
				logger.error("Can't delete file that didn't manage to move from temporary", ignored);
			}
			callback.onException(e);
		}
	}

	@Override
	public void deleteTmp(String fileName, CompletionCallback callback) {
		logger.trace("Deleting temporary file {}", fileName);
		Path path = tmpStorage.resolve(fileName + inProgressExtension);
		try {
			Files.delete(path);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't delete temporary file {}", fileName, e);
			callback.onException(e);
		}
	}

	@Override
	public StreamProducer<ByteBuf> get(String fileName) {
		logger.trace("Streaming file {}", fileName);
		Path destination = fileStorage.resolve(fileName);
		return StreamFileReader.readFileFully(eventloop, executor, bufferSize, destination);
	}

	@Override
	public void delete(String fileName, CompletionCallback callback) {
		logger.trace("Deleting file {}", fileName);
		Path path = fileStorage.resolve(fileName);
		try {
			deleteFile(path);
			callback.onComplete();
		} catch (IOException e) {
			logger.error("Can't delete file {}", fileName, e);
			callback.onException(e);
		}
	}

	@Override
	public void list(ResultCallback<Set<String>> callback) {
		logger.trace("Listing files");
		Set<String> result = new HashSet<>();
		try {
			listFiles(fileStorage, result, "");
			callback.onResult(result);
		} catch (IOException e) {
			logger.error("Can't list files", e);
			callback.onException(e);
		}
	}

	private void listFiles(Path parent, Set<String> files, String previousPath) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(parent)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					if (!path.equals(tmpStorage)) {
						listFiles(path, files, previousPath + path.getFileName().toString() + File.separator);
					}
				} else {
					files.add(previousPath + path.getFileName().toString());
				}
			}
		}
	}

	private Path ensureDestinationDirectory(String path) throws IOException {
		return ensureDirectory(fileStorage, path);
	}

	private Path ensureInProgressDirectory(String path) throws IOException {
		return ensureDirectory(tmpStorage, path + inProgressExtension);
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

	private void deleteFile(Path path) throws IOException {
		if (Files.isDirectory(path)) {
			if (isDirEmpty(path)) {
				Files.delete(path);
			}
			path = path.getParent();
			if (path != null && !path.equals(fileStorage) && !path.equals(tmpStorage)) {
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
		if (Files.exists(tmpStorage)) {
			cleanFolder(tmpStorage);
		} else {
			Files.createDirectory(tmpStorage);
		}
	}

	private void cleanFolder(Path container) throws IOException {
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(container)) {
			for (Path path : directoryStream) {
				if (Files.isDirectory(path) && path.iterator().hasNext()) {
					cleanFolder(path);
				}
				Files.delete(path);
			}
		}
	}
}