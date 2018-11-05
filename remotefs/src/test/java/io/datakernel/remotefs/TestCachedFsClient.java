/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ActivePromisesRule;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
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
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class TestCachedFsClient {
	public static final Function<SerialSupplier<ByteBuf>, Promise<Void>> RECYCLING_FUNCTION = supplier -> supplier.streamTo(SerialConsumer.of(AsyncConsumer.of(ByteBuf::recycle)));
	public static final Function<SerialSupplier<ByteBuf>, Promise<String>> TO_STRING = supplier -> supplier.toCollector(ByteBufQueue.collector()).thenApply(buf -> buf.asString(UTF_8));
	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Rule
	public final ByteBufRule byteBufRule = new ByteBufRule();

	private final static InetSocketAddress address = new InetSocketAddress("localhost", 23343);
	private Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

	private Path cacheStorage;
	private Path serverStorage;
	private Path cacheTestFile;
	private Path serverTestFile;
	private ExecutorService executor;
	private CachedFsClient cacheRemote;
	private RemoteFsServer server;
	private String testTxtContent;

	private FsClient main;
	private FsClient cache;

	@Before
	public void setUp() throws Exception {
		cacheStorage = Paths.get(tempFolder.newFolder("cacheStorage").toURI());
		serverStorage = Paths.get(tempFolder.newFolder("serverStorage").toURI());

		//		cacheStorage = Paths.get("/tmp/TEST/cache");
		//		serverStorage = Paths.get("/tmp/TEST/server");
		//		Runtime.getRuntime().exec("rm -r /tmp/TEST").waitFor();

		Files.createDirectories(cacheStorage);
		Files.createDirectories(serverStorage);

		cacheTestFile = cacheStorage.resolve("test.txt");

		serverTestFile = serverStorage.resolve("test.txt");
		testTxtContent = "line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8";
		Files.write(serverTestFile, testTxtContent.getBytes(UTF_8));

		executor = Executors.newCachedThreadPool();
		main = RemoteFsClient.create(eventloop, address);
		cache = LocalFsClient.create(eventloop, executor, cacheStorage);
		cacheRemote = CachedFsClient.create(main, cache, CachedFsClient.lruCompare()).with(MemSize.kilobytes(50));
		server = RemoteFsServer.create(eventloop, executor, serverStorage).withListenAddress(address);
		server.listen();
	}

	@After
	public void tearDown() {
		executor.shutdown();
	}

	@Test
	public void testDownloadFileNotInCache() {
		cacheRemote.download("test.txt")
				.thenCompose(TO_STRING)
				.whenComplete(($, e) -> server.close())
				.whenComplete(assertComplete(s -> assertEquals(testTxtContent, s)))
				.thenCompose($ -> cache.download("test.txt")
						.thenCompose(TO_STRING)
						.whenComplete(assertComplete(s -> assertEquals(testTxtContent, s))));

		eventloop.run();
	}

	@Test
	public void testDownloadFileNotInCacheWithOffsetAndLength() {
		cacheRemote.download("test.txt", 1, 2)
				.thenCompose(TO_STRING)
				.whenComplete((res, err) -> {
					server.close();
					assertEquals(res, "in");
					assertFalse(Files.exists(cacheStorage.resolve("test.txt")));
				})
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFilePartlyInCache() throws IOException {
		byte[] bytes = "line1\nline2\nline3".getBytes(UTF_8);
		Files.write(cacheTestFile, bytes);
		assertArrayEquals(bytes, Files.readAllBytes(cacheTestFile));

		cacheRemote.download("test.txt")
				.thenCompose(TO_STRING)
				.whenComplete(($, e) -> server.close())
				.whenComplete(assertComplete(s -> assertEquals(testTxtContent, s)))
				.thenCompose($ -> cache.download("test.txt"))
				.thenCompose(TO_STRING)
				.whenComplete(assertComplete(s -> assertEquals(testTxtContent, s)));

		eventloop.run();
	}

	@Test
	public void testDownloadFilePartlyInCacheWithOffsetAndLength() throws IOException {
		Files.write(cacheTestFile, "line1\nline2\nline3".getBytes(UTF_8));

		cacheRemote.download("test.txt", 1, 2)
				.thenCompose(TO_STRING)
				.whenComplete(($, e) -> server.close())
				.whenComplete((res, err) -> assertEquals("in", res))
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFileFullyInCache() throws IOException {
		Files.copy(serverTestFile, cacheTestFile);
		cacheRemote.download("test.txt")
				.thenCompose(TO_STRING)
				.whenComplete(($, e) -> server.close())
				.whenResult(s -> assertEquals(testTxtContent, s))
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFileFullyInCacheWithOffsetAndLength() throws IOException {
		Files.copy(serverTestFile, cacheTestFile);
		cacheRemote.download("test.txt", 1, 2)
				.thenCompose(TO_STRING)
				.whenComplete((res, err) -> assertEquals("in", res))
				.whenComplete(assertComplete())
				.whenComplete(($, e) -> server.close());

		eventloop.run();
	}

	@Test
	public void testDownloadFileNotOnServer() throws IOException {
		Path filePath = cacheStorage.resolve("cacheOnly.txt");
		String fileContent = "This file is stored only in cache";
		Files.write(filePath, fileContent.getBytes());
		cacheRemote.download("cacheOnly.txt")
				.thenCompose(TO_STRING)
				.whenComplete(($, e) -> server.close())
				.whenComplete(assertComplete())
				.whenResult(s -> assertEquals(fileContent, s));

		eventloop.run();
	}

	@Test
	public void testDownloadFileNotOnServerWithOffsetAndLength() throws IOException {
		Path filePath = cacheStorage.resolve("cacheOnly.txt");
		String fileContent = "This file is stored only in cache";
		Files.write(filePath, fileContent.getBytes());
		cacheRemote.download("cacheOnly.txt", 1, 2)
				.thenCompose(TO_STRING)
				.whenComplete((res, err) -> assertEquals("hi", res))
				.whenComplete(assertComplete())
				.whenComplete(($, e) -> server.close());

		eventloop.run();
	}

	@Test
	public void testList() throws IOException {
		// Creating directories
		Files.createDirectories(cacheStorage.resolve("a"));
		Files.createDirectories(cacheStorage.resolve("b"));
		Files.createDirectories(cacheStorage.resolve("c/d"));
		Files.createDirectories(serverStorage.resolve("a"));
		Files.createDirectories(serverStorage.resolve("b"));

		// Adding 4 NEW files to cache, total is 5
		Files.write(cacheStorage.resolve("test1.txt"), "11".getBytes());
		Files.write(cacheStorage.resolve("a/test2.txt"), "22".getBytes());
		Files.write(cacheStorage.resolve("b/test3.txt"), "33".getBytes());
		Files.write(cacheStorage.resolve("c/d/test4.txt"), "44".getBytes());

		//		// Adding 3 NEW files to server, total is 8
		Files.write(serverStorage.resolve("_test1.txt"), "11new".getBytes());
		Files.write(serverStorage.resolve("a/_test2.txt"), "22new".getBytes());
		Files.write(serverStorage.resolve("b/_test3.txt"), "33new".getBytes());

		// Adding 2 SAME (as in cache) files to server, total is still 8
		Files.write(serverStorage.resolve("test1.txt"), "11server".getBytes());
		Files.write(serverStorage.resolve("a/test2.txt"), "22server".getBytes());

		// Adding 1 new file, tottal is 9
		Files.write(serverStorage.resolve("newFile.txt"), "New data".getBytes());

		cacheRemote.list()
				.whenResult(list -> assertEquals(list.size(), 9))
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());
		eventloop.run();
	}

	@Test
	public void testGetMetadata() throws IOException {
		Files.write(serverStorage.resolve("newFile.txt"), "Initial data\n".getBytes(), StandardOpenOption.CREATE_NEW);

		cacheRemote.getMetadata("newFile.txt")
				.thenApply(metadata -> {
					assertNotNull(metadata);
					return metadata.getSize();
				})
				.thenCompose(oldSize -> {
					try {
						Files.write(serverStorage.resolve("newFile.txt"), "Appended data\n".getBytes(), StandardOpenOption.APPEND);
					} catch (IOException e) {
						throw new AssertionError(e);
					}
					return cacheRemote.getMetadata("newFile.txt")
							.whenResult(newMetadata ->
									assertTrue("New metadata is not greater than old one", newMetadata.getSize() > oldSize));
				})
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testGetMetadataOfNonExistingFile() {
		cacheRemote.getMetadata("nonExisting.txt")
				.whenComplete((res, err) -> assertNull(res))
				.whenComplete(assertComplete())
				.whenComplete(($, err) -> server.close());

		eventloop.run();
	}

	@Test
	public void testEnsureCapacity() throws IOException {
		// Current cache size limit is 50 KB
		MemSize sizeLimit = MemSize.kilobytes(50);

		// ~100 KB
		initializeCacheDownloadFiles(20, "testFile_");

		cacheRemote.getTotalCacheSize()
				.whenResult(cacheSize -> {
					assertTrue(sizeLimit.toLong() >= cacheSize.toLong());
					server.close();
				})
				.whenResult($ -> server.close());

		eventloop.run();
	}

	@Test
	public void testEnsureCapacityWithOldFiles() throws IOException {
		// Current cache size limit is 50 Kb
		MemSize sizeLimit = MemSize.kilobytes(50);

		// 49 Kb old file in cache
		byte[] data = new byte[(int) (4.9 * 1024)];
		new Random().nextBytes(data);
		Files.write(cacheStorage.resolve("oldFile.txt"), data);

		cacheRemote.start().whenResult(r -> server.close());

		eventloop.run();

		server.listen();

		// ~10 KB
		initializeCacheDownloadFiles(2, "tetsFile_");

		cacheRemote.getTotalCacheSize()
				.whenResult(cacheSize -> {
					assertTrue(sizeLimit.toLong() >= cacheSize.toLong());
					server.close();
				})
				.whenComplete(($, e) -> server.close());

		eventloop.run();
	}

	@Test
	public void testSetCacheSizeLimit() throws IOException {
		// Current cache size limit is 50 Kb
		MemSize sizeLimit = MemSize.kilobytes(50);

		// 100Kb
		initializeCacheDownloadFiles(20, "testFile_");

		cacheRemote.getTotalCacheSize()
				.whenResult(cacheSize -> {
					assertTrue(sizeLimit.toLong() >= cacheSize.toLong());
					server.close();
				})
				.whenResult($ -> server.close());

		MemSize newSizeLimit = MemSize.kilobytes(20);

		cacheRemote.setCacheSizeLimit(newSizeLimit)
				.whenResult(r -> cacheRemote.getTotalCacheSize()
						.whenResult(cacheSize -> {
							assertTrue(newSizeLimit.toLong() >= cacheSize.toLong());
							server.close();
						})
						.whenResult($ -> server.close())
				);

		eventloop.run();
	}

	@Test
	public void testGetTotalCacheSize() throws IOException {
		initializeCacheFolder();
		cacheRemote.getTotalCacheSize().whenResult(r -> server.close());
		eventloop.run();
	}

	@Test
	public void testStart() throws IOException {
		initializeCacheFolder();

		cacheRemote.start()
				.whenComplete(assertComplete())
				.thenApply($ -> cacheRemote.getTotalCacheSize()
						.whenResult(r -> assertTrue(r.toLong() < cacheRemote.getCacheSizeLimit().toLong())))
				.whenResult($ -> server.close());
		eventloop.run();
	}

	@Test
	public void testStartWithoutMemSize() {
		cacheRemote = CachedFsClient.create(main, cache, CachedFsClient.lruCompare());
		try {
			cacheRemote.start();
		} catch (IllegalStateException e) {
			assertEquals(IllegalStateException.class, e.getClass());
			server.close();
		}

		eventloop.run();
	}

	@Test
	public void testLRUComparator() throws IOException {
		// LRU is a default cache policy here
		cacheRemote.setCacheSizeLimit(MemSize.kilobytes(100));

		// 25 KB
		initializeCacheDownloadFiles(5, "testFile_");

		// 100 KB
		initializeFiles(20, "newTestFile_");
		downloadFiles(20, 1, "newTestFile_");

		cache.list()
				.whenResult(list -> list.forEach(val -> assertTrue(val.getFilename().startsWith("new"))));
		eventloop.run();
	}

	@Test
	public void testLFUComparator() throws IOException {
		cacheRemote = CachedFsClient.create(main, cache, CachedFsClient.lfuCompare()).with(MemSize.kilobytes(25));

		// 20 KB
		initializeCacheDownloadFiles(4, "testFile_");

		// increasing cache hit values
		downloadFiles(4, 2, "testFile_");

		// 10 KB
		initializeCacheDownloadFiles(2, "newTestFile_");

		cache.list()
				.whenResult(list -> assertEquals(1, list.stream().filter(fileMetadata -> fileMetadata.getFilename().startsWith("new")).count()));
		eventloop.run();
	}

	@Test
	public void testSizeComparator() throws IOException {
		// create 49 KB File
		byte[] fileData = new byte[49 * 1024];
		new Random().nextBytes(fileData);
		Files.write(cacheStorage.resolve("bigFile.txt"), fileData);

		cacheRemote.start().thenCompose($ -> cacheRemote.download("bigFile.txt"))
				.thenCompose(RECYCLING_FUNCTION)
				.thenCompose($ -> cacheRemote.download("bigFile.txt"))
				.thenCompose(RECYCLING_FUNCTION)
				.whenComplete(($, e) -> server.close());
		eventloop.run();

		server.listen();

		// 1 file
		initializeCacheDownloadFiles(1, "testFile_");

		cache.list()
				.whenResult(list ->
						list.forEach(value -> {
							System.out.println(value);
							assertTrue(value.getFilename().startsWith("test"));
						}))
				.whenComplete(($, e) -> server.close());
		eventloop.run();
	}

	@Test
	public void testEnsureSpaceLoadFactor() throws IOException {
		// 50 KB - CacheSizeLimit
		initializeCacheDownloadFiles(10, "test");
		// create 1 byte file - should trigger ensureCapacity() -> resulting size should be ~ 35Kb
		byte[] fileData = new byte[1];
		new Random().nextBytes(fileData);
		Files.write(serverStorage.resolve("tiny.txt"), fileData);

		server.listen();

		cacheRemote.download("tiny.txt")
				.whenComplete(assertComplete($ -> System.out.println("DOWNLOAD COMPLETED")))
				.thenCompose(RECYCLING_FUNCTION)
				.whenComplete(($, e) -> server.close())
				.thenCompose($ -> cacheRemote.getTotalCacheSize())
				.whenComplete(assertComplete(size -> assertEquals(35 * 1024 + 1, size.toLong())));
		eventloop.run();
	}

	@Test
	public void testDelete() throws IOException {
		// 10 KB
		initializeCacheDownloadFiles(2, "test");
		// 10 KB
		initializeCacheDownloadFiles(2, "toDelete");

		server.listen();

		cache.list()
				.whenComplete(assertComplete(list -> assertEquals(4, list.size())))
				.thenCompose($ -> cacheRemote.delete("toDelete*"))
				.thenCompose($ -> cache.list())
				.whenComplete(assertComplete(list -> {
					assertEquals(2, list.size());
					list.forEach(file -> assertTrue(file.getFilename().startsWith("test")));
				}))
				.whenComplete(($, e) -> server.close());
		eventloop.run();
	}

	private void initializeCacheFolder() throws IOException {
		Files.createDirectories(cacheStorage.resolve("a"));
		Files.createDirectories(cacheStorage.resolve("b"));
		Files.createDirectories(cacheStorage.resolve("c/d"));

		// 49 bytes
		Files.write(cacheStorage.resolve("test1.txt"), "File".getBytes());
		Files.write(cacheStorage.resolve("a/test2.txt"), "Second File".getBytes());
		Files.write(cacheStorage.resolve("b/test3.txt"), "Yet Another File".getBytes());
		Files.write(cacheStorage.resolve("c/d/test4.txt"), "The other one file".getBytes());
	}

	private void initializeCacheDownloadFiles(int numberOfFiles, String prefix) throws IOException {
		initializeFiles(numberOfFiles, prefix);
		downloadFiles(numberOfFiles, 1, prefix);
	}

	private long initializeFiles(int numberOfFiles, String prefix) throws IOException {
		Random random = new Random();
		String[] files = new String[numberOfFiles];
		long sizeAccum = 0;
		for (int i = 0; i < numberOfFiles; i++) {
			int dataSize = 5 * 1024;
			byte[] data = new byte[dataSize];
			random.nextBytes(data);
			files[i] = prefix + i;
			Files.write(serverStorage.resolve(files[i]), data);
			sizeAccum += data.length;
		}
		return sizeAccum;
	}

	private void downloadFiles(int numberOfFiles, int nTimes, String prefix) throws IOException {
		for (int j = 0; j < nTimes; j++) {
			for (int i = 0; i < numberOfFiles; i++) {
				server.listen();
				cacheRemote.download(prefix + i)
						.thenCompose(RECYCLING_FUNCTION)
						.whenComplete(($, e) -> server.close());
				eventloop.run();
			}
		}
	}
}
