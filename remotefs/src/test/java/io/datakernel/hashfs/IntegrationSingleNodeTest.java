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

package io.datakernel.hashfs;

import io.datakernel.FsClient;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.SimpleException;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.StreamProducers.ClosingWithError;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Charsets.UTF_8;
import static io.datakernel.async.AsyncRunnables.runInParallel;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamProducers.ofValue;
import static io.datakernel.stream.file.StreamFileWriter.create;
import static java.nio.file.Files.*;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class IntegrationSingleNodeTest {
	private static final byte[] CONTENT = encodeAscii("content");

	private Replica local = Replica.create("a", new InetSocketAddress("127.0.0.1", 5569), 1.0);

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private Path serverStorage;
	private Path clientStorage;

	@Before
	public void setup() throws IOException {
		clientStorage = Paths.get(temporaryFolder.newFolder("client_storage").toURI());
		serverStorage = Paths.get(temporaryFolder.newFolder("server_storage").toURI());

		Path clientA = clientStorage.resolve("a.txt");
		write(clientA, "this is a.txt in ./this/is directory".getBytes(UTF_8));
		Path clientB = clientStorage.resolve("b.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\r\n");
		}
		write(clientB, sb.toString().getBytes(UTF_8));
		Path clientC = clientStorage.resolve("c.txt");
		write(clientC, new byte[]{});

		Path thisFolder = serverStorage.resolve("this");
		createDirectories(thisFolder);
		Path thisA = thisFolder.resolve("a.txt");
		write(thisA, "Local a.txt".getBytes(UTF_8));
		Path thisG = thisFolder.resolve("g.txt");
		write(thisG, "Local g.txt".getBytes(UTF_8));
		Path d = serverStorage.resolve("d.txt");
		write(d, "Local d.txt".getBytes(UTF_8));
		Path e = serverStorage.resolve("e.txt");
		write(e, "Local e.txt".getBytes(UTF_8));
		Path f = serverStorage.resolve("f.txt");
		write(f, "Local f.txt".getBytes(UTF_8));
	}

	@Test
	public void testUpload() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		final HashFsServer server = createServer(eventloop, executor);
		HashFsClient client = createClient(eventloop);

		server.listen();

		client.upload(
				"this/is/a.txt",
				ofValue(eventloop, ByteBuf.wrapForReading(CONTENT)),
				new AssertingCompletionCallback() {
					@Override
					protected void onComplete() {
						server.close(IgnoreCompletionCallback.create());
					}
				});

		eventloop.run();
		executor.shutdown();

		assertArrayEquals(CONTENT, readAllBytes(serverStorage.resolve("this/is/a.txt")));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testFailedUpload() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		final HashFsServer server = createServer(eventloop, executor);
		final HashFsClient client = createClient(eventloop);
		final StreamProducer<ByteBuf> producer = new ClosingWithError<>(eventloop, new SimpleException("Test Exception"));
		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();

		server.listen();
		client.upload("non_existing_file.txt", producer, new CompletionCallback() {
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
		executor.shutdownNow();

		thrown.expect(ExecutionException.class);
		callback.get();

		Path path = serverStorage.resolve("non_existing_file.txt");
		assertTrue(exists(path));
		assertEquals(0, size(path));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDownload() throws IOException, ExecutionException, InterruptedException {
		final int startPosition = 2;
		byte[] dFileContent = encodeAscii("Local d.txt".substring(startPosition));
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();
		final HashFsServer server = createServer(eventloop, executor);
		HashFsClient client = createClient(eventloop);
		final StreamFileWriter consumerD = create(eventloop, executor, clientStorage.resolve("d_downloaded.txt"));
		final StreamFileWriter consumerG = create(eventloop, executor, clientStorage.resolve("g_downloaded.txt"));

		runInParallel(eventloop,
				new AsyncRunnable() {
					@Override
					public void run(CompletionCallback callback) {
						consumerD.setFlushCallback(callback);
					}
				},
				new AsyncRunnable() {
					@Override
					public void run(CompletionCallback callback) {
						consumerG.setFlushCallback(callback);
					}
				}
		).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server.close(IgnoreCompletionCallback.create());
			}
		});

		server.listen();
		client.download("this/g.txt", 0, streamTo(eventloop, consumerG));
		client.download("d.txt", startPosition, streamTo(eventloop, consumerD));
		eventloop.run();
		executor.shutdown();

		assertArrayEquals(dFileContent, readAllBytes(clientStorage.resolve("d_downloaded.txt")));
		assertArrayEquals(readAllBytes(serverStorage.resolve("this/g.txt")), readAllBytes(clientStorage.resolve("g_downloaded.txt")));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testFailedDownload() throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		final ExecutorService executor = newCachedThreadPool();

		final HashFsServer server = createServer(eventloop, executor);
		final FsClient client = createClient(eventloop);

		server.listen();

		client.download("file_does_not_exist", 0, new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> producer) {
				try {
					StreamFileWriter consumerA = create(eventloop, executor, clientStorage.resolve("file_should_not exist.txt"));
					consumerA.setFlushCallback(new CompletionCallback() {
						@Override
						protected void onException(Exception e) {
							onCompleteOrException();
						}

						@Override
						protected void onComplete() {
							onCompleteOrException();
						}

						void onCompleteOrException() {
							server.close(IgnoreCompletionCallback.create());
						}
					});
					producer.streamTo(consumerA);
				} catch (IOException ignored) {
					// ignored
				}
			}

			@Override
			public void onException(Exception e) {
				server.close(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertFalse(exists(clientStorage.resolve("file_should_not exist.txt")));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDelete() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();

		final HashFsServer server = createServer(eventloop, executor);
		FsClient client = createClient(eventloop);
		server.listen();

		client.delete("this/a.txt", new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				server.close(IgnoreCompletionCallback.create());
			}
		});
		eventloop.run();
		executor.shutdown();

		assertFalse(exists(serverStorage.resolve("this/a.txt")));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testFailedDelete() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();

		final HashFsServer server = createServer(eventloop, executor);
		final FsClient client = createClient(eventloop);
		server.listen();

		final CompletionCallbackFuture callback = CompletionCallbackFuture.create();
		client.delete("not_exist.txt", new CompletionCallback() {
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

		thrown.expect(ExecutionException.class);
		callback.get();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testList() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = newCachedThreadPool();

		final HashFsServer server = createServer(eventloop, executor);
		final FsClient client = createClient(eventloop);

		final List<String> expected = asList("d.txt", "e.txt", "this/g.txt", "f.txt", "this/a.txt");
		final List<String> actual = new ArrayList<>();

		server.listen();

		client.list(new ResultCallback<List<String>>() {
			@Override
			public void onResult(List<String> result) {
				actual.addAll(result);
				server.close(IgnoreCompletionCallback.create());
			}

			@Override
			public void onException(Exception e) {
				server.close(IgnoreCompletionCallback.create());
			}
		});

		eventloop.run();
		executor.shutdown();

		Collections.sort(expected);
		Collections.sort(actual);

		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	private ResultCallback<StreamProducer<ByteBuf>> streamTo(final Eventloop eventloop, final StreamConsumer<ByteBuf> consumer) {
		return new ResultCallback<StreamProducer<ByteBuf>>() {
			@Override
			public void onResult(StreamProducer<ByteBuf> producer) {
				producer.streamTo(consumer);
			}

			@Override
			public void onException(Exception e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
			}
		};
	}

	private HashFsClient createClient(Eventloop eventloop) {
		return HashFsClient.create(eventloop, asList(local))
				.withBaseRetryTimeout(1)
				.withMaxRetryAttempts(1);
	}

	private HashFsServer createServer(Eventloop eventloop, ExecutorService executor) {
		LocalReplica localReplica = LocalReplica.create(eventloop, executor, serverStorage, asList(local), local);
		localReplica.start(IgnoreCompletionCallback.create());
		return HashFsServer.create(eventloop, localReplica)
				.withListenAddress(local.getAddress());
	}
}