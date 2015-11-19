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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Charsets.UTF_8;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class IntegrationSingleNodeTest {
	private static final Logger logger = LoggerFactory.getLogger(IntegrationSingleNodeTest.class);

	private ServerInfo local = new ServerInfo(0, new InetSocketAddress("127.0.0.1", 4455), 1.0);
	private static Path serverStorage;
	private static Path clientStorage;

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() throws IOException {
		clientStorage = Paths.get(temporaryFolder.newFolder("client_storage").toURI());
		serverStorage = Paths.get(temporaryFolder.newFolder("server_storage").toURI());

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
		Files.createFile(clientC);

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
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		final NioService server = getServer(eventloop, executor);
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
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		final NioService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final StreamProducer<ByteBuf> producer = new StreamProducers.ClosingWithError<>(eventloop, new Exception("Test Exception"));

		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.upload("non_existing_file.txt", producer, new CompletionCallback() {
					@Override
					public void onComplete() {
						logger.info("Miracle happened!... or ... bug!?");
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
						logger.info("Failed to upload: {}", e.getMessage());
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
				});
			}

			@Override
			public void onException(Exception e) {
				logger.error("Didn't manage to start the server", e);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertFalse(Files.exists(serverStorage.resolve("non_existing_file.txt")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDownload() throws IOException {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		final NioService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final StreamFileWriter consumerG = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve("g_downloaded.txt"), true);
		final StreamFileWriter consumerE = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve("e_downloaded.txt"), true);
		final StreamFileWriter consumerF = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve("f_downloaded.txt"), true);

		consumerE.setFlushCallback(new CompletionCallback() {
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
			}
		});
		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.download("this/g.txt", consumerG);
				client.download("e.txt", consumerE);
				client.download("f.txt", consumerF);
			}

			@Override
			public void onException(Exception e) {
				logger.error("Didn't manage to start the server", e);
			}
		});

		eventloop.run();
		executor.shutdownNow();

		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("g_downloaded.txt").toFile(), serverStorage.resolve("this/g.txt").toFile()));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("e_downloaded.txt").toFile(), serverStorage.resolve("e.txt").toFile()));
		assertTrue(com.google.common.io.Files.equal(clientStorage.resolve("f_downloaded.txt").toFile(), serverStorage.resolve("f.txt").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFailedDownload() throws IOException {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		final NioService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final StreamFileWriter consumerA = StreamFileWriter.createFile(eventloop, executor, clientStorage.resolve("file_should_not exist.txt"), true);
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
		server.start(new CompletionCallback() {
			@Override
			public void onComplete() {
				client.download("file_does_not_exist", consumerA);
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
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		final NioService server = getServer(eventloop, executor);
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
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		final NioService server = getServer(eventloop, executor);
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

	@Test
	public void testList() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();

		final NioService server = getServer(eventloop, executor);
		final FsClient client = getClient(eventloop);

		final Set<String> expected = Sets.newHashSet("this/a.txt", "this/g.txt", "e.txt", "f.txt", "d.txt");
		final Set<String> actual = new HashSet<>();

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

		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	private FsClient getClient(NioEventloop eventloop) {
		return HashFsClient.buildInstance(eventloop, Lists.newArrayList(local))
				.setMaxRetryAttempts(1)
				.build();
	}

	private NioService getServer(NioEventloop eventloop, ExecutorService executor) {
		return HashFsServer.buildInstance(eventloop, executor, serverStorage, local, Sets.newHashSet(local))
				.build();
	}
}