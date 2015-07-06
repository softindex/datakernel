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
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
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

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class SimpleFsServerTest {
	private static final Logger logger = LoggerFactory.getLogger(SimpleFsServerTest.class);

	private static final int LISTEN_PORT = 6432;
	private static final InetSocketAddress address = new InetSocketAddress("127.0.0.1", LISTEN_PORT);
	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();
	private Path dirPath;
	private Path serverStorage;

	@Before
	public void before() throws Exception {

		dirPath = Paths.get(folder.newFolder("test").toURI());
		serverStorage = Paths.get(folder.newFolder("server_storage").toURI());

		Files.createDirectories(dirPath);

		Path t1 = dirPath.resolve("t1");
		Files.write(t1, ("some text\n\nmore text\t\n\n\r").getBytes(Charsets.UTF_8));

		Path t2 = dirPath.resolve("t2");
		Files.write(t2, ("\n\raaa\nbbb").getBytes(Charsets.UTF_8));

		Path t3 = dirPath.resolve("empty_file");
		Files.createFile(t3);

		Path t4 = dirPath.resolve("a b");
		Files.write(t4, ("a\nb\nc").getBytes(Charsets.UTF_8));

		Path t5 = dirPath.resolve("big_file");

		StringBuilder testFileContentBuilder = new StringBuilder();
		for (int i = 0; i < 1000000; i++) {
			testFileContentBuilder.append(i);
			testFileContentBuilder.append("\n");
		}
		Files.write(t5, testFileContentBuilder.toString().getBytes(Charsets.UTF_8));
	}

	@Test
	public void testDeleteFile() throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		Files.createDirectories(serverStorage);
		Files.copy(dirPath.resolve("big_file"), serverStorage.resolve("big_file"));

		prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop);

		final String requestedFile = "big_file";

		final ResultCallback<Boolean> callback = new ResultCallback<Boolean>() {
			@Override
			public void onResult(Boolean result) {

			}

			@Override
			public void onException(Exception exception) {
				logger.error(exception.getMessage(), exception);
			}
		};

		client.deleteFile(address, requestedFile, callback);
		eventloop.run();

		Path pathDeleted = serverStorage.resolve(requestedFile);
		assertFalse(Files.exists(pathDeleted));
	}

	@Test
	public void testDeleteNotExistsFile() throws Exception {
		final NioEventloop eventloop = new NioEventloop();
		prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop);

		final String requestedFile = "big_file_1";

		final ResultCallback<Boolean> callback = new ResultCallback<Boolean>() {
			@Override
			public void onResult(Boolean result) {
				logger.info("File delete status\t" + result);
			}

			@Override
			public void onException(Exception exception) {
				logger.info(exception.getMessage());
			}
		};

		client.deleteFile(address, requestedFile, callback);
		eventloop.run();
	}

	@Test
	public void testFileList() throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		Files.createDirectories(serverStorage);
		Files.copy(dirPath.resolve("t1"), serverStorage.resolve("t1"));
		Files.copy(dirPath.resolve("t2"), serverStorage.resolve("t2"));
		Files.copy(dirPath.resolve("a b"), serverStorage.resolve("a b"));
		Files.copy(dirPath.resolve("empty_file"), serverStorage.resolve("empty_file"));
		Files.copy(dirPath.resolve("big_file"), serverStorage.resolve("big_file"));

		prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop);

		final ResultCallback<List<String>> callback = new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> result) {

				Collections.sort(result);
				List<String> expected = Arrays.asList("t1", "t2", "a b", "empty_file", "big_file");
				Collections.sort(expected);

				assertEquals(expected, result);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Can't get file list", exception);
			}
		};

		client.fileList(address, callback);

		eventloop.run();
	}

	@Test
	public void testUpload() throws Exception {

		final String requestedFile = "t2";
		final String resultFile = "t2_uploaded";

		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();
		prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
				16 * 1024, dirPath.resolve(requestedFile));

		client.write(address, resultFile, new ResultCallback<StreamConsumer<ByteBuf>>() {
			@Override
			public void onResult(StreamConsumer<ByteBuf> result) {
				producer.streamTo(result);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Can't upload", exception);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(dirPath.resolve(requestedFile).toFile(), serverStorage.resolve(resultFile).toFile()));
	}

	@Test
	public void testUploadBig() throws Exception {

		final String requestedFile = "big_file";
		final String resultFile = "big_file_uploaded";

		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();
		prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
				16 * 1024, dirPath.resolve(requestedFile));

		client.write(address, resultFile, new ResultCallback<StreamConsumer<ByteBuf>>() {
			@Override
			public void onResult(StreamConsumer<ByteBuf> result) {
				producer.streamTo(result);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Can't upload", exception);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(dirPath.resolve(requestedFile).toFile(), serverStorage.resolve(resultFile).toFile()));
	}

	@Test
	public void testDownload() throws Exception {

		final String requestedFile = "big_file";
		final String resultFile = "big_file_downloaded";

		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();

		Files.createDirectories(serverStorage);
		Files.copy(dirPath.resolve("big_file"), serverStorage.resolve("big_file"));
		prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop);

		final ResultCallback<StreamProducer<ByteBuf>> callback = new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> result) {
				StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, dirPath.resolve(resultFile));
				result.streamTo(consumer);
			}

			@Override
			public void onException(Exception exception) {
				logger.error(exception.getMessage());
			}
		};

		client.read(address, requestedFile, callback);

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(dirPath.resolve(requestedFile).toFile(), dirPath.resolve(resultFile).toFile()));
	}

	@Test
	public void testDownloadNotExistsFile() throws Exception {

		final String requestedFile = "t2_not_exist";
		final String resultFile = "t2_downloaded";

		final ExecutorService executor = Executors.newCachedThreadPool();

		final NioEventloop eventloop = new NioEventloop();
		prepareServer(eventloop);

		SimpleFsClient client = new SimpleFsClient(eventloop);

		final ResultCallback<StreamProducer<ByteBuf>> callback = new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> result) {
				logger.info("Start download file {}", requestedFile);
				StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, dirPath.resolve(resultFile));
				result.streamTo(consumer);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("Exception on client: {}", exception.getMessage());
			}
		};

		client.read(address, requestedFile, callback);

		eventloop.run();
		executor.shutdown();
	}

	private void prepareServer(NioEventloop eventloop) throws IOException {

		final ExecutorService executor = Executors.newCachedThreadPool();
		SimpleFsServer fileServer = SimpleFsServer.createServer(eventloop, serverStorage, executor);
		fileServer.setListenPort(LISTEN_PORT);
		fileServer.acceptOnce();
		try {
			fileServer.listen();
		} catch (IOException e) {
			logger.error("Can't start listen", e);
		}
	}

}
