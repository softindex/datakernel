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

public class FileSystemImpl implements FileSystem {
	private static final Logger logger = LoggerFactory.getLogger(FileSystemImpl.class);

	private static final String IN_PROGRESS_EXTENSION = ".partial";
	private static final String TMP_DIRECTORY = "tmp";

	private final ExecutorService executor;
	private final NioEventloop eventloop;

	private final Path fileStorage;
	private final Path tmpStorage;
	private final int bufferSize;

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private FileSystemImpl(NioEventloop eventloop, ExecutorService executor, Path fileStorage, int bufferSize) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.fileStorage = fileStorage;
		this.tmpStorage = fileStorage.resolve(TMP_DIRECTORY);
		this.bufferSize = bufferSize;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public boolean canSave(String filePath) {
		boolean exists = Files.exists(fileStorage.resolve(filePath));
		boolean waitsForCommit = Files.exists(tmpStorage.resolve(filePath));
		return !exists && !waitsForCommit;
	}

	@Override
	public void stash(String filePath, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		Path tmpDirectory;
		try {
			tmpDirectory = ensureInProgressDirectory(filePath);
		} catch (IOException e) {
			callback.onException(e);
			return;
		}
		StreamFileWriter diskWrite = StreamFileWriter.createFile(eventloop, executor, tmpDirectory, true);
		producer.streamTo(diskWrite);
		diskWrite.setFlushCallback(callback);
	}

	@Override
	public void save(String filePath, CompletionCallback callback) {
		Path destinationDirectory;
		Path tmpDirectory;
		try {
			tmpDirectory = ensureInProgressDirectory(filePath);
			destinationDirectory = ensureDestinationDirectory(filePath);
			Files.move(tmpDirectory, destinationDirectory);
		} catch (IOException e) {
			callback.onException(e);
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
		Path destination = fileStorage.resolve(filePath);
		try {
			Files.delete(destination);
			callback.onComplete();
		} catch (IOException e) {
			callback.onException(e);
		}
	}

	@Override
	public void listFiles(ResultCallback<Set<String>> result) {
		Set<String> files = new HashSet<>();
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(fileStorage)) {
			for (Path path : directoryStream) {
				files.add(path.getFileName().toString());
			}
			result.onResult(files);
		} catch (IOException e) {
			result.onException(e);
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
}
