package io.datakernel.remotefs;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.SerialConsumer;
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

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class TestCachedFsClient {
	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

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
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.thenRunEx(server::close)
				.whenResult(s -> assertEquals(testTxtContent, s))
				.whenResult(s -> {
					try {
						assertEquals(testTxtContent, new String(Files.readAllBytes(cacheStorage.resolve("test.txt")), UTF_8));
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				})
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFileNotInCacheWithOffsetAndLength() {
		cacheRemote.download("test.txt", 1, 2)
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.thenRunEx(server::close)
				.whenComplete((res, err) -> {
					assertEquals(res, "in");
					assertFalse(Files.exists(cacheStorage.resolve("test.txt")));
				})
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFilePartlyInCache() throws IOException {
		Files.write(cacheTestFile, "line1\nline2\nline3".getBytes(UTF_8));

		cacheRemote.download("test.txt")
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.thenRunEx(server::close)
				.whenResult(s -> assertEquals(testTxtContent, s))
				.thenRun(() -> {
					try {
						assertArrayEquals(Files.readAllBytes(cacheTestFile), testTxtContent.getBytes(UTF_8));
					} catch (IOException e) {
						throw new AssertionError(e);
					}
				})
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFilePartlyInCacheWithOffsetAndLength() throws IOException {
		Files.write(cacheTestFile, "line1\nline2\nline3".getBytes(UTF_8));

		cacheRemote.download("test.txt", 1, 2)
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.thenRunEx(server::close)
				.whenComplete((res, err) -> assertEquals("in", res))
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFileFullyInCache() throws IOException {
		Files.copy(serverTestFile, cacheTestFile);
		cacheRemote.download("test.txt")
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.thenRunEx(server::close)
				.whenResult(s -> assertEquals(testTxtContent, s))
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	public void testDownloadFileFullyInCacheWithOffsetAndLength() throws IOException {
		Files.copy(serverTestFile, cacheTestFile);
		cacheRemote.download("test.txt", 1, 2)
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.whenComplete((res, err) -> assertEquals("in", res))
				.whenComplete(assertComplete())
				.thenRunEx(server::close);

		eventloop.run();
	}

	@Test
	public void testDownloadFileNotOnServer() throws IOException {
		Path filePath = cacheStorage.resolve("cacheOnly.txt");
		String fileContent = "This file is stored only in cache";
		Files.write(filePath, fileContent.getBytes());
		cacheRemote.download("cacheOnly.txt")
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.thenRunEx(server::close)
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
				.thenCompose(producer -> {
					ByteBufQueue q = new ByteBufQueue();
					return producer
							.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)))
							.thenApply($ -> new String(q.takeRemaining().asArray(), UTF_8));
				})
				.whenComplete((res, err) -> assertEquals("hi", res))
				.whenComplete(assertComplete())
				.thenRunEx(server::close);

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
				.thenApply(metadata ->
				{
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
		// Current cache size limit is 50 MB
		MemSize sizeLimit = MemSize.megabytes(50);

		// ~100 MB
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
		// Current cache size limit is 50 MB
		MemSize sizeLimit = MemSize.megabytes(50);

		// 49 Mb old file in cache
		byte[] data = new byte[49 * 1024 * 104];
		new Random().nextBytes(data);
		Files.write(cacheStorage.resolve("oldFile.txt"), data);

		cacheRemote.start().whenResult(r -> server.close());

		eventloop.run();

		server.listen();

		// ~100 MB
		initializeCacheDownloadFiles(20, "tetsFile_");

		cacheRemote.getTotalCacheSize()
				.whenResult(cacheSize -> {
					assertTrue(sizeLimit.toLong() >= cacheSize.toLong());
					server.close();
				})
				.whenResult($ -> server.close());

		eventloop.run();
	}

	@Test
	public void testSetCacheSizeLimit() throws IOException {
		// Current cache size limit is 50 MB
		MemSize sizeLimit = MemSize.megabytes(50);

		// 100MB
		initializeCacheDownloadFiles(20, "testFile_");

		cacheRemote.getTotalCacheSize()
				.whenResult(cacheSize -> {
					assertTrue(sizeLimit.toLong() >= cacheSize.toLong());
					server.close();
				})
				.whenResult($ -> server.close());

		MemSize newSizeLimit = MemSize.megabytes(20);

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
				.whenResult(list -> list.forEach(val -> assertTrue(val.getName().startsWith("new"))));
		eventloop.run();
	}

	@Test
	public void testLFUComparator() throws IOException {
		cacheRemote = CachedFsClient.create(main, cache, CachedFsClient.lfuCompare()).with(MemSize.kilobytes(25));

		// 25 KB
		initializeCacheDownloadFiles(5, "testFile_");

		// increasing cache hit values
		downloadFiles(5, 2, "testFile_");

		// 5 KB
		initializeCacheDownloadFiles(1, "newTestFile_");

		cache.list()
				.whenResult(list -> list.forEach(value -> assertTrue(value.getName().startsWith("test"))));
		eventloop.run();
	}

	@Test
	public void testSizeComparator() throws IOException {

		// create 49 KB File
		byte[] fileData = new byte[49 * 1024];
		new Random().nextBytes(fileData);
		Files.write(cacheStorage.resolve("bigFile.txt"), fileData);

		cacheRemote.download("bigFile.txt").thenCompose($ -> cacheRemote.download("bigFile.txt").whenResult($2 -> server.close()));
		eventloop.run();

		server.listen();

		// 1 file
		initializeCacheDownloadFiles(1, "testFile_");

		cache.list()
				.whenResult(list -> list.forEach(value -> assertTrue(value.getName().startsWith("test"))));
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
						.thenCompose(producer -> {
							ByteBufQueue q = new ByteBufQueue();
							return producer
									.streamTo(SerialConsumer.of(AsyncConsumer.of(q::add)));
						})
						.whenResult($ -> server.close());
				eventloop.run();
			}
		}
	}
}
