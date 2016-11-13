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

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.SimpleException;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileWriter;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncRunnables.runInParallel;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.equalsLowerCaseAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class SimpleFsIntegrationTest {
//	static {
//		ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//		logger.setLevel(Level.TRACE);
//	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final InetSocketAddress address = new InetSocketAddress(5560);

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

		upload(resultFile, bytes, IgnoreCompletionCallback.create());

		assertArrayEquals(readAllBytes(storage.resolve(resultFile)), bytes);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testUploadMultiple() throws IOException {
		int files = 10;
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		final SimpleFsServer server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.listen();
		List<AsyncRunnable> tasks = new ArrayList<>();
		for (int i = 0; i < files; i++) {
			final StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(CONTENT));
			final int finalI = i;
			tasks.add(new AsyncRunnable() {
				@Override
				public void run(CompletionCallback callback) {
					client.upload("file" + finalI, producer, callback);
				}
			});
		}
		runInParallel(eventloop, tasks).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server.close(IgnoreCompletionCallback.create());
			}
		});

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

		upload(resultFile, BIG_FILE, IgnoreCompletionCallback.create());

		assertArrayEquals(readAllBytes(storage.resolve(resultFile)), BIG_FILE);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testUploadLong() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		upload(resultFile, CONTENT, IgnoreCompletionCallback.create());

		assertArrayEquals(readAllBytes(storage.resolve(resultFile)), CONTENT);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testUploadExistingFile() throws IOException {
		String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		final Exception es[] = new Exception[1];

		upload(resultFile, CONTENT, IgnoreCompletionCallback.create());
		upload(resultFile, CONTENT, new ExceptionCallback() {
			@Override
			public void onException(Exception e) {
				es[0] = e;
			}
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
		final SimpleFsServer server = createServer(eventloop, executor);
		SimpleFsClient client = createClient(eventloop);

		server.listen();
		StreamProducer<ByteBuf> producer =
				StreamProducers.concat(eventloop,
						StreamProducers.ofIterable(eventloop, asList(
								ByteBufStrings.wrapUtf8("Test1"),
								ByteBufStrings.wrapUtf8(" Test2"),
								ByteBufStrings.wrapUtf8(" Test3"))),
						StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(BIG_FILE)),
						StreamProducers.<ByteBuf>closingWithError(eventloop, new SimpleException("Test exception")),
						StreamProducers.ofValue(eventloop, ByteBufStrings.wrapUtf8("Test4")));

		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();
		client.upload(resultFile, producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				server.close(IgnoreCompletionCallback.create());
				callback.setComplete();
			}

			@Override
			public void onException(Exception e) {
				server.close(IgnoreCompletionCallback.create());
				callback.setException(e);
			}
		});

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
		callback.get();

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
		SimpleFsClient client = createClient(eventloop);
		final SimpleFsServer server = createServer(eventloop, executor);
		final List<Exception> expected = new ArrayList<>();

		server.listen();
		client.download(file, 0, new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> producer) {
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				expected.add(e);
				server.close(IgnoreCompletionCallback.create());
			}
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
		final SimpleFsClient client = createClient(eventloop);
		final SimpleFsServer server = createServer(eventloop, executor);
		int files = 10;

		server.listen();

		List<AsyncRunnable> tasks = new ArrayList<>();
		for (int i = 0; i < files; i++) {
			final int finalI = i;
			tasks.add(new AsyncRunnable() {
				@Override
				public void run(final CompletionCallback callback) {
					client.download(file, 0, new AssertingResultCallback<StreamProducer<ByteBuf>>() {
						@Override
						public void onResult(StreamProducer<ByteBuf> producer) {
							try {
								producer.streamTo(StreamFileWriter.create(eventloop, executor, storage.resolve("file" + finalI)));
							} catch (IOException e) {
								this.setException(e);
							}
							callback.setComplete();
						}
					});
				}
			});
		}
		runInParallel(eventloop, tasks).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server.close(IgnoreCompletionCallback.create());
			}
		});

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
		SimpleFsClient client = createClient(eventloop);
		SimpleFsServer server = createServer(eventloop, executor);
		server.listen();

		client.delete(file, new CloseCompletionCallback(server, IgnoreCompletionCallback.create()));

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
		SimpleFsClient client = createClient(eventloop);
		final SimpleFsServer server = createServer(eventloop, executor);
		server.listen();

		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();
		client.delete(file, new CompletionCallback() {
			@Override
			public void onComplete() {
				callback.setComplete();
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				callback.setException(e);
				server.close(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		executor.shutdown();

		thrown.expect(ExecutionException.class);
		thrown.expectCause(new BaseMatcher<Throwable>() {
			@Override
			public boolean matches(Object item) {
				return item instanceof Exception && ((Exception) item)
						.getMessage().equals(storage.resolve(file).toAbsolutePath().toString());
			}

			@Override
			public void describeTo(Description description) {
				// empty
			}
		});
		callback.get();

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

		final SimpleFsServer server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.listen();
		client.list(new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> result) {
				actual.addAll(result);
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception ignored) {
				server.close(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		executor.shutdownNow();

		Collections.sort(actual);
		Collections.sort(expected);
		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private void upload(String resultFile, byte[] bytes, ExceptionCallback callback) throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		final SimpleFsServer server = createServer(eventloop, executor);
		SimpleFsClient client = createClient(eventloop);

		server.listen();
		StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(bytes));
		client.upload(resultFile, producer, new CloseCompletionCallback(server, callback));
		eventloop.run();
		executor.shutdown();
	}

	private List<ByteBuf> download(String file, long startPosition) throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		SimpleFsClient client = createClient(eventloop);
		final SimpleFsServer server = createServer(eventloop, executor);
		final List<ByteBuf> expected = new ArrayList<>();

		server.listen();
		client.download(file, startPosition, new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> producer) {
				producer.streamTo(StreamConsumers.toList(eventloop, expected));
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				server.close(IgnoreCompletionCallback.create());
			}
		});
		eventloop.run();
		executor.shutdown();
		return expected;
	}

	private SimpleFsClient createClient(Eventloop eventloop) {
		return SimpleFsClient.create(eventloop, address);
	}

	private SimpleFsServer createServer(Eventloop eventloop, ExecutorService executor) {
		return SimpleFsServer.create(eventloop, executor, storage)
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

	private static class CloseCompletionCallback extends CompletionCallback {
		private final SimpleFsServer server;
		private final ExceptionCallback callback;

		public CloseCompletionCallback(SimpleFsServer server, ExceptionCallback callback) {
			this.server = server;
			this.callback = callback;
		}

		@Override
		public void onComplete() {
			server.close(IgnoreCompletionCallback.create());
		}

		@Override
		public void onException(Exception e) {
			server.close(IgnoreCompletionCallback.create());
			callback.setException(e);
		}
	}
}