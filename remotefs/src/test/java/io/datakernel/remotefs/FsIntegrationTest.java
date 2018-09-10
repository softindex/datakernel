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

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.SerialSuppliers;
import io.datakernel.serial.file.SerialFileWriter;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.datakernel.bytebuf.ByteBufStrings.equalsLowerCaseAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class FsIntegrationTest {
	private static final InetSocketAddress address = new InetSocketAddress("localhost", 5560);
	private static final byte[] BIG_FILE = new byte[2 * 1024 * 1024]; // 2 MB

	static {
		TestUtils.enableLogging();
		ThreadLocalRandom.current().nextBytes(BIG_FILE);
	}

	private static final byte[] CONTENT = "content".getBytes(UTF_8);

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path storage;
	private RemoteFsServer server;
	private FsClient client;
	private Eventloop eventloop;
	private ExecutorService executor;

	@Before
	public void setup() throws IOException {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		executor = newCachedThreadPool();

//		storage = Paths.get(temporaryFolder.newFolder("server_storage").toURI());
		try {
			Runtime.getRuntime().exec("rm -r /tmp/TESTS").waitFor();
		} catch (InterruptedException e) {
			System.out.println("removal interrupted: " + e.getMessage());
		}
		storage = Paths.get("/tmp/TESTS");

		server = RemoteFsServer.create(eventloop, executor, storage).withListenAddress(address);
		server.listen();
		client = RemoteFsClient.create(eventloop, address);

//		eventloop.delayBackground(2000, () -> Assert.fail("Timeout"));
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void testUpload() throws IOException {
		String resultFile = "file_uploaded.txt";

		upload(resultFile, CONTENT)
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());
		eventloop.run();

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadMultiple() throws IOException {
		int files = 10;

		Stages.all(IntStream.range(0, 10)
				.mapToObj(i -> SerialSupplier.of(ByteBuf.wrapForReading(CONTENT)).streamTo(client.uploadSerial("file" + i))))
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		for (int i = 0; i < files; i++) {
			assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve("file" + i)));
		}
	}

	@Test
	public void testUploadBigFile() throws IOException {
		String resultFile = "big file_uploaded.txt";

		upload(resultFile, BIG_FILE)
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());
		eventloop.run();

		assertArrayEquals(BIG_FILE, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadLong() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		upload(resultFile, CONTENT)
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());
		eventloop.run();

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadExistingFile() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		upload(resultFile, CONTENT)
				.thenCompose($ -> upload(resultFile, CONTENT))
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertFailure(RemoteFsException.class, "FileAlreadyExistsException"));

		eventloop.run();

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadServerFail() {

		upload("../../nonlocal/../file.txt", CONTENT)
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertFailure(RemoteFsException.class, "File .*? goes outside of the storage directory"));

		eventloop.run();
	}

	@Test
	public void testOnClientExceptionWhileUploading() {
		String resultFile = "upload_with_exceptions.txt";

		ByteBuf test4 = wrapUtf8("Test4");

		SerialSuppliers.concat(
				SerialSupplier.of(wrapUtf8("Test1"), wrapUtf8(" Test2"), wrapUtf8(" Test3")),
				SerialSupplier.of(ByteBuf.wrapForReading(BIG_FILE)),
				SerialSupplier.ofException(new StacklessException("Test exception")),
				SerialSupplier.of(test4))
				.streamTo(client.uploadSerial(resultFile))
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertFailure(StacklessException.class, "Test exception"));

		eventloop.run();
		test4.recycle();

		assertTrue(Files.exists(storage.resolve(resultFile)));
	}

	private ByteBuf download(String file) {
		ByteBufQueue queue = new ByteBufQueue();

		client.downloadSerial(file)
				.streamTo(SerialConsumer.of(AsyncConsumer.of(queue::add)))
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();
		return queue.takeRemaining();
	}

	@Test
	public void testDownload() throws Exception {
		String file = "file1_downloaded.txt";
		Files.write(storage.resolve(file), CONTENT);

		ByteBuf expected = download(file);
		assertTrue(equalsLowerCaseAscii(CONTENT, expected.array(), 0, 7));
		expected.recycle();
	}

	@Test
	public void testDownloadLong() throws Exception {
		String file = "this/is/not/empty/directory/file.txt";
		Files.createDirectories(storage.resolve("this/is/not/empty/directory"));
		Files.write(storage.resolve(file), CONTENT);

		ByteBuf expected = download(file);
		assertTrue(equalsLowerCaseAscii(CONTENT, expected.array(), 0, 7));
		expected.recycle();
	}

	@Test
	public void testDownloadNotExist() {
		String file = "file_not_exist_downloaded.txt";
		client.downloadSerial(file).streamTo(SerialConsumer.of($ -> Stage.complete()))
				.whenComplete(($, e) -> server.close())
				.whenComplete(assertFailure(RemoteFsException.class, "File not found"));
		eventloop.run();
	}

	@Test
	public void testManySimultaneousDownloads() throws IOException {
		String file = "some_file.txt";
		Files.write(storage.resolve(file), CONTENT);

		List<Stage<Void>> tasks = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			tasks.add(client.downloadSerial(file).streamTo(SerialFileWriter.create(executor, storage.resolve("file" + i))));
		}

		Stages.all(tasks)
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		for (int i = 0; i < tasks.size(); i++) {
			assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve("file" + i)));
		}
	}

	@Test
	public void testDeleteFile() throws Exception {
		String file = "file.txt";
		Files.write(storage.resolve(file), CONTENT);

		client.delete(file)
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		assertFalse(Files.exists(storage.resolve(file)));
	}

	@Test
	public void testDeleteMissingFile() {
		String file = "no_file.txt";

		client.delete(file)
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testFileList() throws Exception {
		List<FileMetadata> actual = new ArrayList<>();
		List<FileMetadata> expected = asList(
				new FileMetadata("this/is/not/empty/directory/file1.txt", 7, 0),
				new FileMetadata("file1.txt", 7, 0),
				new FileMetadata("first file.txt", 7, 0)
		);

		Files.createDirectories(storage.resolve("this/is/not/empty/directory/"));
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("file1.txt"), CONTENT);
		Files.write(storage.resolve("first file.txt"), CONTENT);

		client.list()
				.whenComplete((list, throwable) -> {
					if (throwable == null) {
						assert list != null;
						actual.addAll(list);
					}
					server.close();
				});

		eventloop.run();

		Comparator<FileMetadata> comparator = Comparator.comparing(FileMetadata::getName);
		actual.sort(comparator);
		expected.sort(comparator);

		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertTrue(expected.get(i).equalsIgnoringTimestamp(actual.get(i)));
		}
	}

	@Test
	public void testSubfolderClient() throws IOException {
		Files.createDirectories(storage.resolve("this/is/not/empty/directory/"));
		Files.createDirectories(storage.resolve("subfolder1/"));
		Files.createDirectories(storage.resolve("subfolder2/subsubfolder"));
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("file1.txt"), CONTENT);
		Files.write(storage.resolve("first file.txt"), CONTENT);
		Files.write(storage.resolve("subfolder1/file1.txt"), CONTENT);
		Files.write(storage.resolve("subfolder1/first file.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/file1.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/first file.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/subsubfolder/file1.txt"), CONTENT);
		Files.write(storage.resolve("subfolder2/subsubfolder/first file.txt"), CONTENT);

		List<FileMetadata> expected = new ArrayList<>();
		expected.add(new FileMetadata("file1.txt", 7, 0));
		expected.add(new FileMetadata("first file.txt", 7, 0));

		List<FileMetadata> expected2 = new ArrayList<>(expected);
		expected2.add(new FileMetadata("subsubfolder/file1.txt", 7, 0));
		expected2.add(new FileMetadata("subsubfolder/first file.txt", 7, 0));

		List<FileMetadata> actual = new ArrayList<>();
		List<FileMetadata> actual2 = new ArrayList<>();

		Stages.all(client.subfolder("subfolder1").list().whenResult(actual::addAll),
				client.subfolder("subfolder2").list().whenResult(actual2::addAll)).whenComplete(($, err) -> server.close());

		eventloop.run();

		Comparator<FileMetadata> comparator = Comparator.comparing(FileMetadata::getName);
		actual.sort(comparator);
		expected.sort(comparator);
		actual2.sort(comparator);
		expected2.sort(comparator);

		for (int i = 0; i < expected.size(); i++) {
			assertTrue(expected.get(i).toString(), expected.get(i).equalsIgnoringTimestamp(actual.get(i)));
		}
		for (int i = 0; i < expected2.size(); i++) {
			assertTrue(expected2.get(i).toString(), expected2.get(i).equalsIgnoringTimestamp(actual2.get(i)));
		}
	}

	private Stage<Void> upload(String resultFile, byte[] bytes) {
		return SerialSupplier.of(ByteBuf.wrapForReading(bytes)).streamTo(client.uploadSerial(resultFile));
	}
}
