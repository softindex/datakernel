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

package io.datakernel.stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.stream.file.StreamFileReader.readFileFully;
import static java.nio.file.Files.createDirectory;

public class StreamFileWriterStressTest {

	static {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.setLevel(Level.TRACE);
	}

	public static void main(String[] args) throws IOException {
		final ExecutorService executor = Executors.newFixedThreadPool(2);
		final Eventloop eventloop = Eventloop.create();
		final Path storage = Paths.get("./test_data/stress_test/");
		final Path files = storage.resolve("./files/");

		prepareDir(storage);
		prepareBigFiles(files);

		int numberOfThreads = 10;
		final int filesQuantity = 1;
		List<Runnable> list = new ArrayList<>();

		for (int i = 0; i < numberOfThreads; i++) {
			final int finalI = i;
			list.add(new Runnable() {
				@Override
				public void run() {
					for (int j = 0; j < filesQuantity; j++) {
						try {
							StreamFileReader reader = readFileFully(
									eventloop, executor, 16 * 1024, files.resolve("file_" + finalI + ".txt"));
							Path file = storage.resolve("file_" + finalI + "_" + j + "txt");
							StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, file);
							reader.streamTo(writer);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			});
		}

		final ExecutorService mainExecutor = Executors.newCachedThreadPool();

		for (Runnable runnable : list) {
			mainExecutor.submit(runnable);
		}

		eventloop.keepAlive(true);
		eventloop.run();
		executor.shutdown();
		mainExecutor.shutdown();
	}

	private static void prepareBigFiles(Path bigFilesStoragePath) throws IOException {
		if (Files.notExists(bigFilesStoragePath)) {
			Files.createDirectory(bigFilesStoragePath);
			byte[] bytes = new byte[10 * 1024 * 1024]; // 10mb
			Random rand = new Random();
			rand.nextBytes(bytes);
			for (int i = 0; i < 10; i++) {
				Files.write(bigFilesStoragePath.resolve("file_" + i + ".txt"), bytes);
			}
		}
	}

	private static void prepareDir(Path storage) throws IOException {
		if (Files.exists(storage)) {
			Files.walkFileTree(storage, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.delete(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}

			});
		}
		createDirectory(storage);
	}
}
