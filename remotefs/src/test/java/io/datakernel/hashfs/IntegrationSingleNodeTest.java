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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.datakernel.FsClient;
import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.async.SimpleCompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.*;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Charsets.UTF_8;
import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class IntegrationSingleNodeTest {
	private static final Logger logger = LoggerFactory.getLogger(IntegrationSingleNodeTest.class);

	private ServerInfo local = new ServerInfo(0, new InetSocketAddress("127.0.0.1", 4456), 1.0);
	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();
	private static Path serverStorage;
	private static Path clientStorage;

	@BeforeClass
	public static void setupDirs() throws IOException {
		clientStorage = Paths.get(temporaryFolder.newFolder("client_storage").toURI());
		serverStorage = Paths.get(temporaryFolder.newFolder("server_storage").toURI());
	}

	@Before
	public void setup() throws IOException {
		Files.createDirectories(clientStorage);
		Files.createDirectories(serverStorage);

		Path clientA = clientStorage.resolve("a.txt");
		Files.write(clientA, "this is a.txt in ./this/is directory".getBytes(UTF_8));
		Path clientB = clientStorage.resolve("b.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\r\n");
		}
		Files.write(clientB, sb.toString().getBytes(UTF_8));
		Path clientC = clientStorage.resolve("c.txt");
		Files.write(clientC, new byte[]{});

		Path thisFolder = serverStorage.resolve("this");
		Files.createDirectories(thisFolder);
		Path thisA = thisFolder.resolve("a.txt");
		Files.write(thisA, "Local a.txt".getBytes(UTF_8));
		Path thisG = thisFolder.resolve("g.txt");
		Files.write(thisG, "Local g.txt".getBytes(UTF_8));
		Path d = serverStorage.resolve("d.txt");
		Files.write(d, "Local d.txt".getBytes(UTF_8));
		Path e = serverStorage.resolve("e.txt");
		Files.write(e, "Local e.txt".getBytes(UTF_8));
		Path f = serverStorage.resolve("f.txt");
		Files.write(f, "Local f.txt".getBytes(UTF_8));
	}

	@Test
	public void testUpload() throws IOException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		final EventloopService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final StreamProducer<ByteBuf> producerA = StreamFileReader.readFileFully(eventloop, executor, 16 * 256, clientStorage.resolve("a.txt"));
		final StreamProducer<ByteBuf> producerB = StreamFileReader.readFileFully(eventloop, executor, 16 * 256, clientStorage.resolve("b.txt"));
		final StreamProducer<ByteBuf> producerC = StreamFileReader.readFileFully(eventloop, executor, 16 * 256, clientStorage.resolve("c.txt"));

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				logger.info("Server started");
				client.upload("this/is/a.txt", producerA, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Uploaded this/is/a.txt");
					}

					@Override
					public void onException(Exception e) {
						logger.error("Failed to upload file this/is/a.txt", e);
					}
				});
				client.upload("this/is/b.txt", producerB, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Uploaded this/is/b.txt");
					}

					@Override
					public void onException(Exception e) {
						logger.error("Failed to upload file this/is/b.txt", e);
					}
				});
				client.upload("c.txt", producerC, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Uploaded this/is/b.txt");
						server.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Server stooped");
							}

							@Override
							public void onException(Exception e) {
								logger.info("Can't stop the server", e);
							}
						});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Failed to upload file this/is/b.txt", e);
					}
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Didn't manage to start the server", e);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("a.txt").toFile(), serverStorage.resolve("this/is/a.txt").toFile()));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("b.txt").toFile(), serverStorage.resolve("this/is/b.txt").toFile()));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("c.txt").toFile(), serverStorage.resolve("c.txt").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFailedUpload() throws Exception {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		final EventloopService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final StreamProducer<ByteBuf> producer = new StreamProducers.ClosingWithError<>(eventloop, new Exception("Test Exception"));

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.upload("non_existing_file.txt", producer, new SimpleCompletionCallback() {
					@Override
					public void onCompleteOrException() {
						server.stop(ignoreCompletionCallback());
					}
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Didn't manage to start the server", e);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(Files.exists(serverStorage.resolve("non_existing_file.txt")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDownload() throws IOException, ExecutionException, InterruptedException {
		String dFileContents = "Local d.txt";
		final int startPosition = 5;
		final ResultCallbackFuture<Long> sizeFuture = new ResultCallbackFuture<>();

		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		final EventloopService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final StreamFileWriter consumerD = StreamFileWriter.create(eventloop, executor, clientStorage.resolve("d_downloaded.txt"));
		final StreamFileWriter consumerG = StreamFileWriter.create(eventloop, executor, clientStorage.resolve("g_downloaded.txt"));
		final StreamFileWriter consumerE = StreamFileWriter.create(eventloop, executor, clientStorage.resolve("e_downloaded.txt"));
		final StreamFileWriter consumerF = StreamFileWriter.create(eventloop, executor, clientStorage.resolve("f_downloaded.txt"));

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.download("this/g.txt", 0, streamTo(consumerG));
				client.download("e.txt", 0, streamTo(consumerE));
				client.download("d.txt", startPosition, new ResultCallback<StreamTransformerWithCounter>() {
					@Override
					public void onResult(StreamTransformerWithCounter result) {
						result.getOutput().streamTo(consumerD);
						result.setPositionCallback(sizeFuture);
					}

					@Override
					public void onException(Exception e) {
						throw new RuntimeException(e);
					}
				});
				client.download("f.txt", 0, streamTo(consumerF));

				consumerD.setFlushCallback(new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Flushed e");
						server.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Stopped");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Can't stop ", e);
							}
						});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't flush the file");
						server.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Stopped");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Can't stop ", e);
							}
						});
					}
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Didn't manage to start the server", e);
			}
		});

		eventloop.run();
		executor.shutdownNow();
		assertEquals(dFileContents.length() - startPosition, sizeFuture.get().longValue());
		assertEquals(dFileContents.substring(startPosition), new String(Files.readAllBytes(clientStorage.resolve("d_downloaded.txt"))));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("g_downloaded.txt").toFile(), serverStorage.resolve("this/g.txt").toFile()));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("e_downloaded.txt").toFile(), serverStorage.resolve("e.txt").toFile()));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("f_downloaded.txt").toFile(), serverStorage.resolve("f.txt").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFailedDownload() throws IOException {
		final Eventloop eventloop = new Eventloop();
		final ExecutorService executor = newCachedThreadPool();

		final EventloopService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.download("file_does_not_exist", 0, new ResultCallback<StreamTransformerWithCounter>() {
					@Override
					public void onResult(StreamTransformerWithCounter result) {
						final StreamFileWriter consumerA;
						try {
							consumerA = StreamFileWriter.create(eventloop, executor, clientStorage.resolve("file_should_not exist.txt"));
							consumerA.setFlushCallback(new CompletionCallback() {
								@Override
								public void onComplete() {
									logger.info("Can't flush the file");
								}

								@Override
								public void onException(Exception e) {
									server.stop(new CompletionCallback() {
										@Override
										public void onComplete() {
											logger.info("Stopped");
										}

										@Override
										public void onException(Exception e) {
											logger.error("Can't stop ", e);
										}
									});
									logger.error("Can't flush the file");
								}
							});
							result.getOutput().streamTo(consumerA);
						} catch (IOException ignored) {
							// ignored
						}
					}

					@Override
					public void onException(Exception e) {
						server.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Stopped");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Can't stop ", e);
							}
						});
					}
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Didn't manage to start the server", e);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertFalse(Files.exists(clientStorage.resolve("file_should_not exist.txt")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDelete() throws IOException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		final EventloopService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.delete("this/a.txt", new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Deleted");
						server.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Stopped");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Can't stop ", e);
							}
						});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't delete file ", e);
						fail("Can't end here");
					}
				});
			}

			@Override
			public void onException(Exception exception) {

			}
		});
		eventloop.run();
		assertFalse(Files.exists(serverStorage.resolve("this/a.txt")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFailedDelete() throws IOException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		final EventloopService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.delete("not_exist.txt", new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Impossible situation: deleted non existing file");
						fail("Can't end here");
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't delete file ", e);
						server.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Stopped");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Can't stop ", e);
							}
						});
					}
				});
			}

			@Override
			public void onException(Exception exception) {

			}
		});
		eventloop.run();
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Ignore
	@Test
	public void testList() throws Exception {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = newCachedThreadPool();

		final EventloopService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final List<String> expected = Lists.newArrayList("d.txt", "e.txt", "this/g.txt", "non_existing_file.txt", "f.txt", "this/a.txt");
		final List<String> actual = new ArrayList<>();

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.list(new ResultCallback<List<String>>() {
					@Override
					public void onResult(List<String> result) {
						actual.addAll(result);
						server.stop(new CompletionCallback() {
							@Override
							public void onComplete() {
								logger.info("Stopped");
							}

							@Override
							public void onException(Exception e) {
								logger.error("Can't stop ", e);
							}
						});
					}

					@Override
					public void onException(Exception e) {
						logger.error("Can't list files");
					}
				});
			}

			@Override
			public void onException(Exception exception) {

			}
		});
		eventloop.run();

		Collections.sort(expected);
		Collections.sort(actual);

		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private ResultCallback<StreamTransformerWithCounter> streamTo(final StreamFileWriter consumerG) {
		return new ResultCallback<StreamTransformerWithCounter>() {
			@Override
			public void onResult(StreamTransformerWithCounter result) {
				result.getOutput().streamTo(consumerG);
			}

			@Override
			public void onException(Exception e) {
				throw new RuntimeException(e);
			}
		};
	}

	private FsClient getClient(Eventloop eventloop) {
		return HashFsClient.build(eventloop, Lists.newArrayList(local))
				.setMaxRetryAttempts(1)
				.build();
	}

	private EventloopService getServer(Eventloop eventloop, ExecutorService executor) {
		return HashFsServer.newInstance(eventloop, executor, serverStorage, local, Sets.newHashSet(local));
	}
}