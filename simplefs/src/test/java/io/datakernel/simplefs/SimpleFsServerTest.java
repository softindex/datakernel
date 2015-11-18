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

import com.google.common.collect.Lists;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.util.ByteBufStrings;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Charsets.UTF_8;
import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class SimpleFsServerTest {
	private static final InetSocketAddress address = new InetSocketAddress(5560);

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private static Path clientStorage;
	private static Path serverStorage;

	@Before
	public void before() throws IOException {
		clientStorage = Paths.get(temporaryFolder.newFolder("client_storage").toURI());
		serverStorage = Paths.get(temporaryFolder.newFolder("server_storage").toURI());

		Files.createDirectories(clientStorage);
		Files.createDirectories(serverStorage);

		createFile(clientStorage, "file1.txt", "file1");
		createFile(clientStorage, "file2.txt", "file2");
		createFile(clientStorage, "empty.txt", "");

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1000000; i++) {
			sb.append(i).append("\n");
		}
		createFile(clientStorage, "big file.txt", sb.toString());

		Files.copy(clientStorage.resolve("file1.txt"), serverStorage.resolve("file1.txt"));
		Files.createDirectories(serverStorage.resolve("this/is/not/empty/directory/"));
		Files.copy(clientStorage.resolve("file1.txt"), serverStorage.resolve("this/is/not/empty/directory/file1.txt"));
		Files.copy(clientStorage.resolve("file1.txt"), serverStorage.resolve("first file.txt"));
	}

	@Test
	public void testStart() throws IOException {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();
		Path container = temporaryFolder.getRoot().toPath();
		Path differentStorage = container.resolve("another/path/in/file/system");
		Path differentTmpStorage = container.resolve("path/for/tmp/files");

		SimpleFsServer server = SimpleFsServer.buildInstance(eventloop, executor, differentStorage)
				.setTmpStorage(differentTmpStorage)
				.build();

		server.start(ignoreCompletionCallback());
		server.stop(ignoreCompletionCallback());

		assertTrue(Files.exists(differentStorage));
		assertTrue(Files.exists(differentTmpStorage));
	}

	@Test
	public void testUpload() throws IOException {
		String requestedFile = "file1.txt";
		final String resultFile = "file1_uploaded.txt";

		upload(requestedFile, resultFile);

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), serverStorage.resolve(resultFile).toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testUploadMultiple() throws IOException {
		final int files = 10;
		final NioEventloop eventloop = new NioEventloop();
		final byte[] bytes = "Test data!".getBytes(UTF_8);

		ExecutorService executor = newCachedThreadPool();
		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final CompletionCallback callback = AsyncCallbacks.waitAll(files, new CompletionCallback() {
			@Override
			public void onComplete() {
				server.stop(ignoreCompletionCallback());
			}

			@Override
			public void onException(Exception e) {
				server.stop(ignoreCompletionCallback());
			}
		});

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				for (int i = 0; i < files; i++) {
					StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrap(bytes));
					client.upload("test" + i, producer, callback);
				}
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdown();

		for (int i = 0; i < files; i++) {
			assertArrayEquals(bytes, Files.readAllBytes(serverStorage.resolve("test" + i)));
		}
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testUploadBigFile() throws IOException {
		String requestedFile = "big file.txt";
		final String resultFile = "big file_uploaded.txt";

		upload(requestedFile, resultFile);

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), serverStorage.resolve(resultFile).toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testUploadLong() throws IOException {
		String requestedFile = "file2.txt";
		final String resultFile = "this/is/not/empty/directory/2/file2_uploaded.txt";

		upload(requestedFile, resultFile);

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), serverStorage.resolve(resultFile).toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFileAlreadyExist() throws IOException {
		String requestedFile = "file1.txt";
		final String resultFile = "first file.txt";

		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();
		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
				16 * 1024, clientStorage.resolve(requestedFile));

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.upload(resultFile, producer, new CompletionCallback() {
					@Override
					public void onComplete() {
						fail("Can't end here");
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testClientException() throws IOException {
		final String resultFile = "upload_with_exceptions.txt";

		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();
		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamProducer<ByteBuf> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop,
						asList(ByteBufStrings.wrapUTF8("Test1"), ByteBufStrings.wrapUTF8(" Test2"), ByteBufStrings.wrapUTF8(" Test3"))),
				StreamProducers.<ByteBuf>closingWithError(eventloop, new Exception("Test exception")));

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.upload(resultFile, producer, new CompletionCallback() {
					@Override
					public void onComplete() {
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDownload() throws Exception {
		final String requestedFile = "file1.txt";
		String resultFile = "file1_downloaded.txt";

		download(requestedFile, resultFile);

		assertTrue(com.google.common.io.Files.equal(serverStorage.resolve(requestedFile).toFile(), clientStorage.resolve(resultFile).toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDownloadLong() throws Exception {
		final String requestedFile = "this/is/not/empty/directory/file1.txt";
		String resultFile = "file1_downloaded.txt";

		download(requestedFile, resultFile);

		assertTrue(com.google.common.io.Files.equal(serverStorage.resolve(requestedFile).toFile(), clientStorage.resolve(resultFile).toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDownloadNotExist() throws Exception {
		final String requestedFile = "file_not_exist.txt";
		String resultFile = "file_not_exist_downloaded.txt";

		ExecutorService executor = newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile), true);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				consumer.setFlushCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
				client.download(requestedFile, consumer);
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdown();

		assertFalse(Files.exists(clientStorage.resolve(resultFile)));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testTwoSimultaneousDownloads() throws IOException {
		final String requestedFile = "file1.txt";
		String resultFile1 = "file1_downloaded1";
		String resultFile2 = "file1_downloaded2";

		ExecutorService executor = newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamFileWriter consumer1 = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile1), true);
		final StreamFileWriter consumer2 = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile2), true);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				consumer1.setFlushCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
				client.download(requestedFile, consumer1);

				consumer2.setFlushCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
				client.download(requestedFile, consumer2);

			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), clientStorage.resolve(resultFile1).toFile()));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve(requestedFile).toFile(), clientStorage.resolve(resultFile2).toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteFile() throws Exception {
		final String requestedFile = "first file.txt";

		deleteFile(requestedFile);

		assertFalse(Files.exists(serverStorage.resolve(requestedFile)));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteLong() throws Exception {
		final String requestedFile = "this/is/not/empty/directory/file1.txt";

		deleteFile(requestedFile);

		assertFalse(Files.exists(serverStorage.resolve(requestedFile)));
		assertFalse(Files.exists(serverStorage.resolve("this")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteMissingFile() throws Exception {
		final String requestedFile = "big_file_not_exist.txt";

		ExecutorService executor = newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.delete(requestedFile, new CompletionCallback() {
					@Override
					public void onComplete() {
						fail("Should not end here");
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFileList() throws Exception {
		ExecutorService executor = newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		final List<String> actual = new ArrayList<>();
		final List<String> expected = Lists.newArrayList("this/is/not/empty/directory/file1.txt", "file1.txt", "first file.txt");

		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.list(new ResultCallback<List<String>>() {
					@Override
					public void onResult(List<String> result) {
						actual.addAll(result);
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();

		Collections.sort(actual);
		Collections.sort(expected);
		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private static void createFile(Path container, String fileName, String text) throws IOException {
		Path path = container.resolve(fileName);
		Files.write(path, text.getBytes(UTF_8));
	}

	private void upload(String requestedFile, final String resultFile) {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();
		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
				16 * 1024, clientStorage.resolve(requestedFile));

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.upload(resultFile, producer, new CompletionCallback() {
					@Override
					public void onComplete() {
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();
	}

	private void download(final String requestedFile, String resultFile) {
		ExecutorService executor = newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();
		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve(resultFile), true);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				consumer.setFlushCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
				client.download(requestedFile, consumer);
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();
	}

	private void deleteFile(final String requestedFile) {
		ExecutorService executor = newCachedThreadPool();
		NioEventloop eventloop = new NioEventloop();

		final NioService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.delete(requestedFile, new CompletionCallback() {
					@Override
					public void onComplete() {
						server.stop(ignoreCompletionCallback());
					}

					@Override
					public void onException(Exception ignored) {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception ignored) {

			}
		});

		eventloop.run();
		executor.shutdownNow();
	}

	private static NioService createServer(NioEventloop eventloop, ExecutorService executor) {
		return SimpleFsServer.buildInstance(eventloop, executor, serverStorage)
				.specifyListenAddress(address)
				.build();
	}

	private static SimpleFsClient createClient(NioEventloop eventloop) {
		return SimpleFsClient.buildInstance(eventloop, address).build();

	}
}