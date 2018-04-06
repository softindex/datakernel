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

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducerWithResult;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.util.MemSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.nio.file.StandardOpenOption.*;

public final class FileManager {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final OpenOption[] CREATE_OPTIONS = new OpenOption[]{WRITE, CREATE_NEW};

	private final Eventloop eventloop;
	private final ExecutorService executor;
	private final Path storagePath;

	private MemSize readerBufferSize = MemSize.kilobytes(256);

	private FileManager(Eventloop eventloop, ExecutorService executor, Path storagePath) {
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

	public static FileManager create(Eventloop eventloop, ExecutorService executor, Path storagePath) {
		return new FileManager(eventloop, executor, storagePath);
	}

	public Stage<StreamProducerWithResult<ByteBuf, Void>> get(String fileName, long startPosition) {
		logger.trace("downloading file: {}, position: {}", fileName, startPosition);
		return AsyncFile.openAsync(executor, storagePath.resolve(fileName), new OpenOption[]{READ})
				.thenApply(result -> {
					logger.trace("{} opened", result);
					return StreamFileReader.readFile(result).withBufferSize(readerBufferSize).withStartingPosition(startPosition)
							.withEndOfStreamAsResult()
							.withLateBinding();
				});
	}

	public Stage<StreamConsumerWithResult<ByteBuf, Void>> save(String fileName) {
		logger.trace("uploading file: {}", fileName);
		return ensureDirectoryAsync(storagePath, fileName).thenCompose(path -> {
			logger.trace("ensured directory: {}", storagePath);
			return AsyncFile.openAsync(executor, path, CREATE_OPTIONS)
					.thenApply(file -> {
						logger.trace("{} opened", file);
						return StreamFileWriter.create(file)
								.withForceOnClose(true)
								.withFlushAsResult()
								.withLateBinding();
					});
		});
	}

	public Stage<Void> delete(String fileName) {
		logger.trace("deleting file: {}", fileName);
		return AsyncFile.delete(executor, storagePath.resolve(fileName));
	}

	public Stage<Long> size(String fileName) {
		logger.trace("calculating file size: {}", fileName);
		return AsyncFile.size(executor, storagePath.resolve(fileName));
	}

	public Stage<List<String>> scanAsync() {
		return Stage.ofCallable(executor, () -> {
			logger.trace("listing files");
			List<String> result = new ArrayList<>();
			doScan(storagePath, result, "");
			return result;
		});
	}

	public Stage<Void> move(String fileName, String targetName, CopyOption... options) {
		logger.trace("move {} to {}", fileName, targetName);

		String targetAbsolutePath = storagePath.resolve(targetName).toFile().getAbsolutePath();
		String storageAbsolutePath = storagePath.toFile().getAbsolutePath();
		if (!targetAbsolutePath.startsWith(storageAbsolutePath)) {
			return Stage.ofException(new IllegalArgumentException(targetName));
		}

		return ensureDirectoryAsync(storagePath, targetName).thenCompose(target -> {
			try {
				Path source = storagePath.resolve(fileName);
				Files.setLastModifiedTime(source, FileTime.fromMillis(eventloop.currentTimeMillis()));
				return AsyncFile.move(eventloop, executor, source, target, options);
			} catch (IOException e) {
				return Stage.ofException(e);
			}
		});
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

	private Stage<Path> ensureDirectoryAsync(Path container, String path) {
		return Stage.ofCallable(executor, () -> ensureDirectory(container, path));
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
