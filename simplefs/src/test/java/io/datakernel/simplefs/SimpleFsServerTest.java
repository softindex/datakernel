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

package io.datakernel.simplefs;

import com.google.common.base.Charsets;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class SimpleFsServerTest {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServerTest.class);

	private static final int LISTEN_PORT = 6432;
	private static final InetSocketAddress address = new InetSocketAddress("127.0.0.1", LISTEN_PORT);

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private Path clientStorage;
	private Path serverStorage;

	@Before
	public void before() throws Exception {
		clientStorage = Paths.get(temporaryFolder.newFolder("test").toURI());
		serverStorage = Paths.get(temporaryFolder.newFolder("server_storage").toURI());

		Files.createDirectories(clientStorage);
		Files.createDirectories(serverStorage);

		Path t1 = clientStorage.resolve("t1");
		Files.write(t1, ("some text\n\nmore text\t\n\n\r").getBytes(Charsets.UTF_8));

		Path t2 = clientStorage.resolve("t2");
		Files.write(t2, ("\n\raaa\nbbb").getBytes(Charsets.UTF_8));

		Path emptyFile = clientStorage.resolve("empty_file");
		Files.createFile(emptyFile);

		Path ab = clientStorage.resolve("a b");
		Files.write(ab, ("a\nb\nc").getBytes(Charsets.UTF_8));

		Path bigFile = clientStorage.resolve("big_file");
		StringBuilder testFileContentBuilder = new StringBuilder();
		for (int i = 0; i < 1000000; i++) {
			testFileContentBuilder.append(i).append("\n");
		}
		Files.write(bigFile, testFileContentBuilder.toString().getBytes(Charsets.UTF_8));
	}

	@Test
	public void testDeleteFile() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		String requestedFile = "big_file";

		Files.copy(clientStorage.resolve(requestedFile), serverStorage.resolve(requestedFile));

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final CompletionCallback callback = new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("File deleted");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage(), e);
				stop(fileServer);
			}
		};
		client.deleteFile(requestedFile, callback);
		eventloop.run();

		Path pathDeleted = serverStorage.resolve(requestedFile);
		assertFalse(Files.exists(pathDeleted));
	}

	@Test
	public void testDeleteMissingFile() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		String requestedFile = "big_file_not_exist";

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final CompletionCallback callback = new CompletionCallback() {
			@Override
			public void onComplete() {
				stop(fileServer);
				assertFalse(true);
				logger.info("Should not end there");
			}

			@Override
			public void onException(Exception e) {
				stop(fileServer);
				logger.info(e.getMessage());
			}
		};
		client.deleteFile(requestedFile, callback);

		eventloop.run();
	}

	@Test
	public void testFileList() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		uploadFiles(eventloop, "t1", "t2", "a b", "empty_file", "big_file");

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final ResultCallback<List<String>> callback = new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> result) {
				stop(fileServer);
				Collections.sort(result);
				List<String> expected = Arrays.asList("t1", "t2", "a b", "empty_file", "big_file");
				Collections.sort(expected);
				assertEquals(expected, result);
			}

			@Override
			public void onException(Exception exception) {
				stop(fileServer);
				logger.error("Can't get file list", exception);
			}
		};

		client.listFiles(callback);
		eventloop.run();
	}

	@Test
	public void testUpload() throws Exception {
		String requestedFile = "t2";
		String resultFile = "t2_uploaded";

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
				16 * 1024, clientStorage.resolve(requestedFile));

		client.upload(resultFile, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("File uploaded");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage(), e);
				stop(fileServer);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), serverStorage.resolve(resultFile).toFile()));
	}

	@Test
	public void testUploadMultiple() throws Exception {
		int files = 10;
		NioEventloop eventloop = new NioEventloop();
		byte[] bytes = "Hello, World!".getBytes();

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		CompletionCallback callback = AsyncCallbacks.waitAll(files, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("All files uploaded");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage(), e);
				stop(fileServer);
			}
		});

		for (int i = 0; i < files; i++) {
			StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrap(bytes));
			client.upload("file" + i, producer, callback);
		}

		eventloop.run();

		for (int i = 0; i < files; i++) {
			assertArrayEquals(bytes, Files.readAllBytes(serverStorage.resolve("file" + i)));
		}

		return;
	}

	@Test
	public void testUploadBig() throws Exception {
		String requestedFile = "big_file";
		String resultFile = "big_file_uploaded";

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
				16 * 1024, clientStorage.resolve(requestedFile));

		client.upload(resultFile, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Big file uploaded");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage(), e);
				stop(fileServer);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), serverStorage.resolve(resultFile).toFile()));
	}

	@Test
	public void testDownload() throws Exception {
		String requestedFile = "big_file";
		String resultFile = "big_file_downloaded";

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		Files.copy(clientStorage.resolve(requestedFile), serverStorage.resolve(requestedFile));

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile));
		consumer.setFlushCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.error("File downloaded");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage(), e);
				stop(fileServer);
			}
		});

		client.download(requestedFile, consumer);

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), clientStorage.resolve(resultFile).toFile()));
	}

	@Test
	public void testActionsAfterStop() {
		// TODO
	}

	@Test
	public void testDownloadNotExistsFile() throws Exception {
		String requestedFile = "t2_not_exist";
		String resultFile = "t2_downloaded";

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		final SimpleFsServer fileServer = prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile));
		consumer.setFlushCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				// file server won't be stopped
				fail("Can't download non existing file");
				stop(fileServer);
			}

			@Override
			public void onException(Exception exception) {
				logger.info(exception.getMessage());
				stop(fileServer);
			}
		});

		client.download(requestedFile, consumer);

		eventloop.run();
		executor.shutdown();
	}

	@Test
	public void testTwoSimultaneousDownloads() throws IOException {
		String requestedFile = "big_file";
		String resultFile1 = "big_file_downloaded1";
		String resultFile2 = "big_file_downloaded2";

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		Files.copy(clientStorage.resolve(requestedFile), serverStorage.resolve(requestedFile));

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		StreamFileWriter consumer1 = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile1));
		StreamFileWriter consumer2 = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile2));
		consumer1.setFlushCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("File 1 downloaded");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage(), e);
			}
		});
		consumer2.setFlushCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("File 2 downloaded");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error(e.getMessage(), e);
				stop(fileServer);
			}
		});

		client.download(requestedFile, consumer1);
		client.download(requestedFile, consumer2);

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), clientStorage.resolve(resultFile1).toFile()));
	}

	private void uploadFiles(NioEventloop eventloop, final String... fileNames) throws IOException {
		final SimpleFsServer fileServer = prepareServer(eventloop);
		ExecutorService executor = Executors.newCachedThreadPool();
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		for (int i = 0; i < fileNames.length; i++) {
			final String fileName = fileNames[i];
			final int counter = i + 1;
			final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
					16 * 1024, clientStorage.resolve(fileName));

			client.upload(fileName, producer, new CompletionCallback() {
				@Override
				public void onComplete() {
					logger.info("Uploaded file: {}", fileName);
					if (counter == fileNames.length)
						stop(fileServer);
				}

				@Override
				public void onException(Exception exception) {
					logger.error("Can't upload: {}", fileName, exception);
				}
			});
		}

		eventloop.run();
		executor.shutdownNow();
	}

	private SimpleFsServer prepareServer(NioEventloop eventloop) throws IOException {
		final ExecutorService executor = Executors.newCachedThreadPool();
		SimpleFsServer fileServer = SimpleFsServer.createServer(eventloop, serverStorage, executor);
		fileServer.setListenPort(LISTEN_PORT);
		try {
			fileServer.listen();
		} catch (IOException e) {
			logger.error("Can't start listen", e);
		}
		fileServer.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Started server");
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to start server", e);
			}
		});
		return fileServer;
	}

	private void stop(SimpleFsServer server) {
		server.stop(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Server has been stopped");
			}

			@Override
			public void onException(Exception exception) {
				logger.info("Failed to stop server");
			}
		});
	}
}
