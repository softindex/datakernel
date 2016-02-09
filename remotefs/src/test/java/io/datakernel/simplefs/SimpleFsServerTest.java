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
import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.util.ByteBufStrings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Charsets.UTF_8;
import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.stream.file.StreamFileReader.readFileFrom;
import static io.datakernel.stream.file.StreamFileReader.readFileFully;
import static io.datakernel.stream.file.StreamFileWriter.create;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class SimpleFsServerTest {
	private static final InetSocketAddress address = new InetSocketAddress(5560);

	@Rule
	public ExpectedException thrown = ExpectedException.none();

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
	public void testStartStop() throws IOException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();
		Path container = temporaryFolder.getRoot().toPath();
		Path differentStorage = container.resolve("another/path/in/file/system");

		SimpleFsServer server = SimpleFsServer.build(eventloop, executor, differentStorage)
				.build();

		server.start(ignoreCompletionCallback());
		server.stop(ignoreCompletionCallback());

		assertTrue(Files.exists(differentStorage));
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
		final Eventloop eventloop = new Eventloop();
		final byte[] bytes = "Test data!".getBytes(UTF_8);

		ExecutorService executor = newCachedThreadPool();
		final EventloopService server = createServer(eventloop, executor);
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

		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();
		final EventloopService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamProducer<ByteBuf> producer = readFileFrom(eventloop, executor,
				16 * 1024, clientStorage.resolve(requestedFile), 0);

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
						fail("Can't end here");
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

		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();
		final EventloopService server = createServer(eventloop, executor);
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
				fail("Unexpected");
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
	public void testDownloadWithPositions() throws Exception {
		String fileContent = "file1";
		final String requestedFile = "file1.txt";
		final String resultFile = "file1_downloaded.txt";
		final int startPosition = 2;

		final ResultCallbackFuture<Long> sizeFuture = new ResultCallbackFuture<>();

		final Eventloop eventloop = new Eventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();
		final SimpleFsServer server = createServer(eventloop, executor);

		final SimpleFsClient client = createClient(eventloop);
		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.download(requestedFile, startPosition, new ForwardingResultCallback<StreamTransformerWithCounter>(this) {
					@Override
					public void onResult(StreamTransformerWithCounter result) {
						try {
							StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, clientStorage.resolve(resultFile));
							result.getOutput().streamTo(writer);
							result.setPositionCallback(sizeFuture);
							writer.setFlushCallback(new ForwardingCompletionCallback(this) {
								@Override
								public void onComplete() {
									server.stop(ignoreCompletionCallback());
								}
							});
						} catch (IOException e) {
							onException(e);
						}
					}
				});
			}

			@Override
			public void onException(Exception exception) {
				server.stop(ignoreCompletionCallback());
			}
		});

		eventloop.run();
		executor.shutdown();

		assertEquals(fileContent.length() - startPosition, sizeFuture.get().longValue());
		assertEquals(fileContent.substring(startPosition), new String(Files.readAllBytes(clientStorage.resolve(resultFile))));
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
		final String resultFile = "file_not_exist_downloaded.txt";

		final ExecutorService executor = newCachedThreadPool();
		final Eventloop eventloop = new Eventloop();
		final EventloopService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {

				client.download(requestedFile, 0, new ForwardingResultCallback<StreamTransformerWithCounter>(this) {
					@Override
					public void onResult(StreamTransformerWithCounter result) {
						try {
							StreamFileWriter consumer = StreamFileWriter.create(eventloop, executor, clientStorage.resolve(requestedFile));
							consumer.setFlushCallback(new ForwardingCompletionCallback(this) {
								@Override
								public void onComplete() {
									server.stop(ignoreCompletionCallback());
								}
							});
							result.getOutput().streamTo(consumer);
						} catch (IOException e) {
							this.onException(e);
						}
					}
				});
			}

			@Override
			public void onException(Exception ignored) {
				server.stop(ignoreCompletionCallback());
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
		Eventloop eventloop = new Eventloop();
		final EventloopService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		final StreamFileWriter consumer1 = create(eventloop, executor, clientStorage.resolve(resultFile1));
		final StreamFileWriter consumer2 = create(eventloop, executor, clientStorage.resolve(resultFile2));

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
				client.download(requestedFile, 0, streamTo(consumer1));

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
				client.download(requestedFile, 0, streamTo(consumer2));

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
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteMissingFile() throws Exception {
		final String requestedFile = "big_file_not_exist.txt";

		ExecutorService executor = newCachedThreadPool();
		Eventloop eventloop = new Eventloop();

		final EventloopService server = createServer(eventloop, executor);
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
		Eventloop eventloop = new Eventloop();

		final List<String> actual = new ArrayList<>();
		final List<String> expected = Lists.newArrayList("this/is/not/empty/directory/file1.txt", "file1.txt", "first file.txt");

		final EventloopService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.list(new ResultCallback<List<String>>() {
					@Override
					public void onResult(List<String> result) {
						System.out.println("Received: " + result);
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
		final Eventloop eventloop = new Eventloop();
		final ExecutorService executor = newCachedThreadPool();
		final EventloopService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		AsyncFile.open(eventloop, executor, clientStorage.resolve(requestedFile), new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
			@Override
			public void onResult(AsyncFile result) {
				final StreamFileReader producer = readFileFully(eventloop, result, 16 * 1024);

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
			}

			@Override
			public void onException(Exception exception) {

			}
		});

		eventloop.run();
		executor.shutdownNow();
	}

	private void download(String requestedFile, String resultFile) {
		download(requestedFile, 0, resultFile);
	}

	private void download(final String requestedFile, final long startPosition, final String resultFile) {
		final ExecutorService executor = newCachedThreadPool();
		final Eventloop eventloop = new Eventloop();
		final EventloopService server = createServer(eventloop, executor);
		final SimpleFsClient client = createClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.download(requestedFile, startPosition, new ResultCallback<StreamTransformerWithCounter>() {
					@Override
					public void onResult(final StreamTransformerWithCounter producer) {
						AsyncFile.open(eventloop, executor, clientStorage.resolve(resultFile), new OpenOption[]{CREATE_NEW, WRITE, TRUNCATE_EXISTING},
								new ForwardingResultCallback<AsyncFile>(this) {
									@Override
									public void onResult(AsyncFile result) {
										final StreamFileWriter writer = StreamFileWriter.create(eventloop, result);
										producer.getOutput().streamTo(writer);
										writer.setFlushCallback(new ForwardingCompletionCallback(this) {
											@Override
											public void onComplete() {
												server.stop(ignoreCompletionCallback());
											}
										});
									}
								});

					}

					@Override
					public void onException(Exception e) {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception ignored) {
				// ignored
			}
		});

		eventloop.run();
		executor.shutdownNow();
	}

	private void deleteFile(final String requestedFile) {
		ExecutorService executor = newCachedThreadPool();
		Eventloop eventloop = new Eventloop();

		final EventloopService server = createServer(eventloop, executor);
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

	private ResultCallback<StreamTransformerWithCounter> streamTo(final StreamFileWriter consumer) {
		return new ResultCallback<StreamTransformerWithCounter>() {
			@Override
			public void onResult(StreamTransformerWithCounter result) {
				result.getOutput().streamTo(consumer);
			}

			@Override
			public void onException(Exception exception) {
				throw new RuntimeException("Can't get stream producer");
			}
		};
	}

	private static SimpleFsServer createServer(Eventloop eventloop, ExecutorService executor) {
		return SimpleFsServer.build(eventloop, executor, serverStorage)
				.listenAddress(address)
				.build();
	}

	private static SimpleFsClient createClient(Eventloop eventloop) {
		return SimpleFsClient.newInstance(eventloop, address);
	}
}