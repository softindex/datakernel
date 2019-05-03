/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.util.Tuple2;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.remotefs.FsClient.BAD_PATH;
import static io.datakernel.util.CollectionUtils.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public final class FsIntegrationTest {
	private static final InetSocketAddress address = new InetSocketAddress("localhost", 5560);
	private static final byte[] BIG_FILE = new byte[2 * 1024 * 1024]; // 2 MB
	private static final byte[] CONTENT = "content".getBytes(UTF_8);

	static {
		ThreadLocalRandom.current().nextBytes(BIG_FILE);
	}

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path storage;
	private RemoteFsServer server;
	private FsClient client;
	private Executor executor;

	@Before
	public void setup() throws IOException {
		executor = newCachedThreadPool();

		storage = temporaryFolder.newFolder("server_storage").toPath();
		server = RemoteFsServer.create(Eventloop.getCurrentEventloop(), executor, storage).withListenAddress(address);
		server.listen();
		client = RemoteFsClient.create(Eventloop.getCurrentEventloop(), address);
	}

	@Test
	public void testUpload() throws IOException {
		String resultFile = "file_uploaded.txt";

		await(upload(resultFile, CONTENT)
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadMultiple() throws IOException {
		int files = 10;

		await(Promises.all(IntStream.range(0, 10)
				.mapToObj(i -> ChannelSupplier.of(ByteBuf.wrapForReading(CONTENT))
						.streamTo(ChannelConsumer.ofPromise(client.upload("file" + i)))))
				.whenComplete(($, e) -> server.close()));

		for (int i = 0; i < files; i++) {
			assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve("file" + i)));
		}
	}

	@Test
	public void testUploadBigFile() throws IOException {
		String resultFile = "big file_uploaded.txt";

		await(upload(resultFile, BIG_FILE)
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals(BIG_FILE, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadLong() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		await(upload(resultFile, CONTENT)
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve(resultFile)));
	}

	@Test
	public void testUploadServerFail() {
		Throwable exception = awaitException(upload("../../nonlocal/../file.txt", CONTENT)
				.whenComplete(($, e) -> server.close()));

		assertSame(BAD_PATH, exception);
	}

	@Test
	public void testOnClientExceptionWhileUploading() throws IOException {
		String resultFile = "upload_with_exceptions.txt";

		ByteBuf test4 = wrapUtf8("Test4");

		ChannelSupplier<ByteBuf> supplier = ChannelSuppliers.concat(
				ChannelSupplier.of(wrapUtf8("Test1"), wrapUtf8(" Test2"), wrapUtf8(" Test3")).async(),
				ChannelSupplier.of(ByteBuf.wrapForReading(BIG_FILE)),
				ChannelSupplier.ofException(new StacklessException(FsIntegrationTest.class, "Test exception")),
				ChannelSupplier.of(test4));

		Throwable exception = awaitException(supplier.streamTo(ChannelConsumer.ofPromise(client.upload(resultFile)))
				.whenComplete(($, e) -> server.close()));

		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString("Test exception"));

		test4.recycle();
		ByteBufQueue queue = new ByteBufQueue();
		queue.addAll(asList(wrapUtf8("Test1 Test2 Test3"), ByteBuf.wrapForReading(BIG_FILE)));
		assertArrayEquals(queue.takeRemaining().asArray(), Files.readAllBytes(storage.resolve(resultFile)));
	}

	private Promise<ByteBuf> download(String file) {
		return client.download(file)
				.then(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(($, e) -> server.close());
	}

	@Test
	public void testDownload() throws Exception {
		String file = "file1_downloaded.txt";
		Files.write(storage.resolve(file), CONTENT);

		ByteBuf result = await(download(file));

		assertArrayEquals(CONTENT, result.asArray());
	}

	@Test
	public void testDownloadLong() throws Exception {
		String file = "this/is/not/empty/directory/file.txt";
		Files.createDirectories(storage.resolve("this/is/not/empty/directory"));
		Files.write(storage.resolve(file), CONTENT);

		ByteBuf result = await(download(file));

		assertArrayEquals(CONTENT, result.asArray());
	}

	@Test
	public void testDownloadNotExist() {
		String file = "file_not_exist_downloaded.txt";
		Throwable exception = awaitException(ChannelSupplier.ofPromise(client.download(file))
				.streamTo(ChannelConsumer.of($ -> Promise.complete()))
				.whenComplete(($, e) -> server.close()));

		assertThat(exception, instanceOf(StacklessException.class));
		assertThat(exception.getMessage(), containsString("File not found"));
	}

	@Test
	public void testManySimultaneousDownloads() throws IOException {
		String file = "some_file.txt";
		Files.write(storage.resolve(file), CONTENT);

		List<Promise<Void>> tasks = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			tasks.add(ChannelSupplier.ofPromise(client.download(file))
					.streamTo(ChannelFileWriter.create(executor, storage.resolve("file" + i))));
		}

		await(Promises.all(tasks)
				.whenComplete(($, e) -> server.close()));

		for (int i = 0; i < tasks.size(); i++) {
			assertArrayEquals(CONTENT, Files.readAllBytes(storage.resolve("file" + i)));
		}
	}

	@Test
	public void testDeleteFile() throws Exception {
		String file = "file.txt";
		Files.write(storage.resolve(file), CONTENT);

		await(client.delete(file)
				.whenComplete(($, e) -> server.close()));

		assertFalse(Files.exists(storage.resolve(file)));
	}

	@Test
	public void testDeleteMissingFile() {
		String file = "no_file.txt";

		await(client.delete(file)
				.whenComplete(($, e) -> server.close()));
	}

	@Test
	public void testFileList() throws Exception {
		Set<String> expected = set(
				"this/is/not/empty/directory/file1.txt",
				"file1.txt",
				"first file.txt"
		);

		Files.createDirectories(storage.resolve("this/is/not/empty/directory/"));
		for (String filename : expected) {
			Files.write(storage.resolve(filename), CONTENT);
		}

		List<FileMetadata> metadataList = await(client.list("**")
				.whenComplete(($, e) -> server.close()));

		assertEquals(expected, metadataList.stream()
				.map(FileMetadata::getName)
				.collect(toSet()));
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

		Set<String> expected1 = new HashSet<>();
		expected1.add("file1.txt");
		expected1.add("first file.txt");

		Set<String> expected2 = new HashSet<>(expected1);
		expected2.add("subsubfolder/file1.txt");
		expected2.add("subsubfolder/first file.txt");

		Tuple2<List<FileMetadata>, List<FileMetadata>> tuple = await(
				Promises.toTuple(client.subfolder("subfolder1").listEntities("**"), client.subfolder("subfolder2").listEntities("**"))
						.whenComplete(($, e) -> server.close())
		);

		assertEquals(expected1, tuple.getValue1().stream().map(FileMetadata::getName).collect(toSet()));
		assertEquals(expected2, tuple.getValue2().stream().map(FileMetadata::getName).collect(toSet()));
	}

	private Promise<Void> upload(String resultFile, byte[] bytes) {
		return client.upload(resultFile)
				.then(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))::streamTo);
	}
}
