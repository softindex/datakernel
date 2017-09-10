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

import ch.qos.logback.classic.Level;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.AsyncRunnable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.SimpleException;
import io.datakernel.stream.*;
import io.datakernel.stream.file.StreamFileWriter;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncRunnables.runInParallel;
import static io.datakernel.async.SettableStage.immediateFailedStage;
import static io.datakernel.async.SettableStage.immediateStage;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.equalsLowerCaseAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class FsIntegrationTest {
	static {
		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		logger.setLevel(Level.TRACE);
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final InetSocketAddress address = new InetSocketAddress("localhost", 5560);

	private static Path storage;
	private static final byte[] BIG_FILE = createBigByteArray();
	private static final byte[] CONTENT = "content".getBytes(UTF_8);

	@Before
	public void before() throws IOException {
		storage = Paths.get(temporaryFolder.newFolder("server_storage").toURI());
	}

	@Test
	public void testUpload() throws IOException {
		String resultFile = "file_uploaded.txt";
		byte[] bytes = "content".getBytes(UTF_8);

		upload(resultFile, bytes);

		assertArrayEquals(readAllBytes(storage.resolve(resultFile)), bytes);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testUploadMultiple() throws IOException {
		int files = 10;
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		final RemoteFsServer server = createServer(eventloop, executor);
		final RemoteFsClient client = createClient(eventloop);

		server.listen();
		List<AsyncRunnable> tasks = new ArrayList<>();
		for (int i = 0; i < files; i++) {
			final StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(CONTENT));
			final int finalI = i;
			tasks.add(() -> {
				producer.streamTo(client.uploadStream("file" + finalI));
				return immediateStage(null);
			});
		}
		runInParallel(eventloop, tasks).run().whenComplete(AsyncCallbacks.assertBiConsumer($ -> server.close()));

		eventloop.run();
		executor.shutdown();

		for (int i = 0; i < files; i++) {
			assertArrayEquals(CONTENT, readAllBytes(storage.resolve("file" + i)));
		}
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testUploadBigFile() throws IOException {
		String resultFile = "big file_uploaded.txt";
		Random rand = new Random(1L);
		for (int i = 0; i < BIG_FILE.length; i++) {
			BIG_FILE[i] = (byte) (rand.nextInt(256) - 128);
		}

		upload(resultFile, BIG_FILE);

		assertArrayEquals(readAllBytes(storage.resolve(resultFile)), BIG_FILE);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testUploadLong() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		upload(resultFile, CONTENT);

		assertArrayEquals(readAllBytes(storage.resolve(resultFile)), CONTENT);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testUploadExistingFile() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		final Throwable es[] = new Throwable[1];

		upload(resultFile, CONTENT);
		upload(resultFile, CONTENT).whenComplete((aVoid, throwable) -> {
			if (throwable != null) es[0] = throwable;
		});

		assertNotNull(es[0]);
		assertArrayEquals(readAllBytes(storage.resolve(resultFile)), CONTENT);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testOnClientExceptionWhileUploading() throws IOException, ExecutionException, InterruptedException {
		String resultFile = "upload_with_exceptions.txt";

		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		final RemoteFsServer server = createServer(eventloop, executor);
		RemoteFsClient client = createClient(eventloop);

		server.listen();
		StreamProducer<ByteBuf> producer =
				StreamProducers.concat(eventloop,
						StreamProducers.ofIterable(eventloop, asList(
								ByteBufStrings.wrapUtf8("Test1"),
								ByteBufStrings.wrapUtf8(" Test2"),
								ByteBufStrings.wrapUtf8(" Test3"))),
						StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(BIG_FILE)),
						StreamProducers.closingWithError(eventloop, new SimpleException("Test exception")),
						StreamProducers.ofValue(eventloop, ByteBufStrings.wrapUtf8("Test4")));

		StreamConsumerWithResult<ByteBuf, Void> consumer = client.uploadStream(resultFile);
		producer.streamTo(consumer);
		CompletableFuture<Void> future = consumer.getResult().whenComplete(($, throwable) -> server.close()).toCompletableFuture();

		eventloop.run();
		executor.shutdownNow();

		thrown.expect(ExecutionException.class);
		thrown.expectCause(new BaseMatcher<Throwable>() {
			@Override
			public boolean matches(Object item) {
				return item instanceof SimpleException && ((SimpleException) item).getMessage().equals("Test exception");
			}

			@Override
			public void describeTo(Description description) {
				// empty
			}
		});
		future.get();

		assertTrue(Files.exists(storage.resolve(resultFile)));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDownload() throws Exception {
		String file = "file1_downloaded.txt";
		Files.write(storage.resolve(file), CONTENT);

		List<ByteBuf> expected = download(file, 0);

		assertTrue(equalsLowerCaseAscii(CONTENT, expected.get(0).array(), 0, 7));

		// created in 'toList' stream consumer
		for (ByteBuf buf : expected) {
			buf.recycle();
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDownloadWithPositions() throws Exception {
		String file = "file1_downloaded.txt";
		Files.write(storage.resolve(file), CONTENT);

		List<ByteBuf> expected = download(file, 2);

		assertTrue(equalsLowerCaseAscii("ntent".getBytes(UTF_8), expected.get(0).array(), 0, 5));

		// created in 'toList' stream consumer
		for (ByteBuf buf : expected) {
			buf.recycle();
		}
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDownloadLong() throws Exception {
		String file = "this/is/not/empty/directory/file.txt";
		Files.createDirectories(storage.resolve("this/is/not/empty/directory"));
		Files.write(storage.resolve(file), CONTENT);

		List<ByteBuf> expected = download(file, 0);

		assertTrue(equalsLowerCaseAscii(CONTENT, expected.get(0).array(), 0, 7));

		// created in 'toList' stream consumer
		for (ByteBuf buf : expected) {
			buf.recycle();
		}
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDownloadNotExist() throws Exception {
		String file = "file_not_exist_downloaded.txt";
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		RemoteFsClient client = createClient(eventloop);
		final RemoteFsServer server = createServer(eventloop, executor);
		final List<Throwable> expected = new ArrayList<>();

		server.listen();
		StreamProducerWithResult<ByteBuf, Void> producer = client.downloadStream(file, 0);
		producer.streamTo(StreamConsumers.idle());
		producer.getResult().whenComplete(($, throwable) -> {
			if (throwable != null) expected.add(throwable);
			server.close();
		});
		eventloop.run();
		executor.shutdown();

		assertEquals(1, expected.size());
		//noinspection ThrowableResultOfMethodCallIgnored
		assertEquals(expected.get(0).getMessage(), "File not found");
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testManySimultaneousDownloads() throws IOException {
		final String file = "some_file.txt";
		Files.write(storage.resolve(file), CONTENT);
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		final ExecutorService executor = newCachedThreadPool();
		final RemoteFsClient client = createClient(eventloop);
		final RemoteFsServer server = createServer(eventloop, executor);
		int files = 10;

		server.listen();

		List<AsyncRunnable> tasks = new ArrayList<>();
		for (int i = 0; i < files; i++) {
			final int finalI = i;
			tasks.add(() -> {
				StreamProducerWithResult<ByteBuf, Void> producer = client.downloadStream(file, 0);
				try {
					producer.streamTo(StreamFileWriter.create(eventloop, executor, storage.resolve("file" + finalI)));
				} catch (IOException e) {
					return immediateFailedStage(e);
				}
				return producer.getResult();
			});
		}
		runInParallel(eventloop, tasks).run().whenComplete(AsyncCallbacks.assertBiConsumer($ -> server.close()));

		eventloop.run();
		executor.shutdown();

		for (int i = 0; i < files; i++) {
			assertArrayEquals(CONTENT, readAllBytes(storage.resolve("file" + i)));
		}
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDeleteFile() throws Exception {
		String file = "file.txt";
		Files.write(storage.resolve(file), CONTENT);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		RemoteFsClient client = createClient(eventloop);
		RemoteFsServer server = createServer(eventloop, executor);
		server.listen();

		client.delete(file).whenComplete((aVoid, throwable) -> server.close());

		eventloop.run();
		executor.shutdown();

		assertFalse(Files.exists(storage.resolve(file)));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDeleteMissingFile() throws Exception {
		final String file = "no_file.txt";
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		RemoteFsClient client = createClient(eventloop);
		final RemoteFsServer server = createServer(eventloop, executor);
		server.listen();

		final CompletableFuture<Void> future = client.delete(file)
				.whenComplete((aVoid, throwable) -> server.close()).toCompletableFuture();

		eventloop.run();
		executor.shutdown();

		thrown.expect(ExecutionException.class);
		thrown.expectCause(new BaseMatcher<Throwable>() {
			@Override
			public boolean matches(Object item) {
				return item instanceof Exception && ((Exception) item)
						.getMessage().endsWith(storage.resolve(file).toAbsolutePath().toString());
			}

			@Override
			public void describeTo(Description description) {
				// empty
			}
		});
		future.get();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testFileList() throws Exception {
		ExecutorService executor = newCachedThreadPool();
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		final List<String> actual = new ArrayList<>();
		final List<String> expected = asList("this/is/not/empty/directory/file1.txt", "file1.txt", "first file.txt");

		Files.createDirectories(storage.resolve("this/is/not/empty/directory/"));
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("this/is/not/empty/directory/file1.txt"), CONTENT);
		Files.write(storage.resolve("file1.txt"), CONTENT);
		Files.write(storage.resolve("first file.txt"), CONTENT);

		final RemoteFsServer server = createServer(eventloop, executor);
		final RemoteFsClient client = createClient(eventloop);

		server.listen();

		client.list().whenComplete((strings, throwable) -> {
			if (throwable == null) actual.addAll(strings);
			server.close();
		});

		eventloop.run();
		executor.shutdownNow();

		Collections.sort(actual);
		Collections.sort(expected);
		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private CompletionStage<Void> upload(String resultFile, byte[] bytes) throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		RemoteFsServer server = createServer(eventloop, executor);
		RemoteFsClient client = createClient(eventloop);

		server.listen();
		StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(bytes));

		StreamConsumerWithResult<ByteBuf, Void> consumer = client.uploadStream(resultFile);
		producer.streamTo(consumer);
		CompletableFuture<Void> future = consumer.getResult()
				.whenComplete(($, throwable) -> server.close())
				.toCompletableFuture();

		eventloop.run();
		executor.shutdown();
		return future;
	}

	private List<ByteBuf> download(String file, long startPosition) throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		RemoteFsClient client = createClient(eventloop);
		final RemoteFsServer server = createServer(eventloop, executor);
		final List<ByteBuf> expected = new ArrayList<>();

		server.listen();

		StreamProducerWithResult<ByteBuf, Void> producer = client.downloadStream(file, startPosition);
		producer.streamTo(StreamConsumers.toList(eventloop, expected));
		producer.getResult().whenComplete(($, throwable) -> {
			server.close();
		});

		eventloop.run();
		executor.shutdown();
		return expected;
	}

	private RemoteFsClient createClient(Eventloop eventloop) {
		return RemoteFsClient.create(eventloop, address);
	}

	private RemoteFsServer createServer(Eventloop eventloop, ExecutorService executor) {
		return RemoteFsServer.create(eventloop, executor, storage)
				.withListenAddress(address);
	}

	static byte[] createBigByteArray() {
		byte[] bytes = new byte[2 * 1024 * 1024];
		Random rand = new Random(1L);
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte) (rand.nextInt(256) - 128);
		}
		return bytes;

	}

}