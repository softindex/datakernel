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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.util.ByteBufStrings;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
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
		clientStorage = Paths.get(temporaryFolder.newFolder("client_storage").toURI());
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

		client.deleteFile(requestedFile, new CompletionCallback() {
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
		});
		eventloop.run();

		Path pathDeleted = serverStorage.resolve(requestedFile);
		assertFalse(Files.exists(pathDeleted));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteMissingFile() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		String requestedFile = "big_file_not_exist";

		final boolean[] ifDeleted = new boolean[1];

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		client.deleteFile(requestedFile, new CompletionCallback() {
			@Override
			public void onComplete() {
				stop(fileServer);
				ifDeleted[0] = false;
			}

			@Override
			public void onException(Exception e) {
				stop(fileServer);
				ifDeleted[0] = true;
			}
		});

		eventloop.run();
		assertTrue(ifDeleted[0]);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFileList() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		uploadFiles(eventloop, "t1", "t2", "a b", "empty_file", "big_file");

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		List<String> expected = Arrays.asList("t1", "t2", "a b", "empty_file", "big_file");
		final List<String> actual = new ArrayList<>();

		client.listFiles(new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> result) {
				actual.addAll(result);
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				stop(fileServer);
			}
		});
		eventloop.run();

		Collections.sort(actual);
		Collections.sort(expected);
		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
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
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testUploadMultiple() throws Exception {
		int files = 10;
		NioEventloop eventloop = new NioEventloop();
		byte[] bytes = "Hello, World!".getBytes(Charset.forName("UTF-8"));

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

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
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
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
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
				logger.info("File downloaded");
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
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Ignore
	@Test
	public void testActionsAfterStop() {
		fail("Not yet implemented");
	}

	@Test
	public void testDownloadNotExistsFile() throws Exception {
		String requestedFile = "t2_not_exist";
		String resultFile = "t2_downloaded";

		final boolean[] isNotDownloaded = new boolean[1];

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		final SimpleFsServer fileServer = prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile));
		consumer.setFlushCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				stop(fileServer);
				isNotDownloaded[0] = false;
			}

			@Override
			public void onException(Exception e) {
				stop(fileServer);
				isNotDownloaded[0] = true;
			}
		});

		client.download(requestedFile, consumer);

		eventloop.run();
		executor.shutdown();
		assertTrue(isNotDownloaded[0]);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
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
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), clientStorage.resolve(resultFile2).toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Ignore
	@Test
	public void testUploadWithException() throws IOException {
		// TODO (vsavchuk) see SimpleFsServer
		String resultFile = "non_existing_file";

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final StreamProducer<ByteBuf> producer = StreamProducers.closingWithError(eventloop, new Exception("Test exception"));
		client.upload(resultFile, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Big file uploaded");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to upload", e);
				stop(fileServer);
			}
		});

		eventloop.run();
		executor.shutdownNow();
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Ignore
	@Test
	public void testBadUpload() throws IOException {
		// TODO (vsavchuk) Requested

		String resultFile = "non_existing_file";

		ExecutorService executor = Executors.newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		final SimpleFsServer fileServer = prepareServer(eventloop);
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		final StreamProducer<ByteBuf> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop,
						Arrays.asList(ByteBufStrings.wrapUTF8("Test1"), ByteBufStrings.wrapUTF8(" Test2"), ByteBufStrings.wrapUTF8(" Test3"))),
				StreamProducers.<ByteBuf>closingWithError(eventloop, new Exception("Test exception"))
		);

		client.upload(resultFile, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Should not happen");
				stop(fileServer);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Failed to upload", e);
				stop(fileServer);
			}
		});

		eventloop.run();
		executor.shutdownNow();
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
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
				public void onException(Exception e) {
					logger.error("Can't upload: {}", fileName, e);
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
			public void onException(Exception e) {
				logger.info("Failed to stop server");
			}
		});
	}
}
