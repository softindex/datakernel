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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.ExecutorRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.remotefs.FsClient.LENGTH_TOO_BIG;
import static io.datakernel.remotefs.FsClient.OFFSET_TOO_BIG;
import static io.datakernel.test.TestUtils.getFreePort;
import static io.datakernel.test.rules.ExecutorRule.getExecutor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;

public final class TestPartialRemoteFs {
	private static final int PORT = getFreePort();
	private static final String FILE = "file.txt";
	private static final byte[] CONTENT = "test content of the file".getBytes(UTF_8);

	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", PORT);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final ExecutorRule executorRule = new ExecutorRule();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private RemoteFsServer server;
	private RemoteFsClient client;

	private Path serverStorage;
	private Path clientStorage;

	@Before
	public void setup() throws IOException {
		Executor executor = getExecutor();

		serverStorage = tempFolder.newFolder().toPath();
		clientStorage = tempFolder.newFolder().toPath();
		server = RemoteFsServer.create(Eventloop.getCurrentEventloop(), executor, serverStorage).withListenAddress(ADDRESS);
		server.listen();
		client = RemoteFsClient.create(Eventloop.getCurrentEventloop(), ADDRESS);

		Files.write(serverStorage.resolve(FILE), CONTENT);
	}

	@Test
	public void justDownload() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE))
				.streamTo(ChannelFileWriter.open(getExecutor(), clientStorage.resolve(FILE)))
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals(CONTENT, Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void ensuredUpload() throws IOException {
		byte[] data = new byte[10 * (1 << 20)]; // 10 mb
		ThreadLocalRandom.current().nextBytes(data);

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(ByteBuf.wrapForReading(data));
		ChannelConsumer<ByteBuf> consumer = ChannelConsumer.ofPromise(client.upload("test_big_file.bin"));

		await(supplier.streamTo(consumer)
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals(data, Files.readAllBytes(serverStorage.resolve("test_big_file.bin")));
	}

	@Test
	public void downloadPrefix() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE, 0, 12))
				.streamTo(ChannelFileWriter.open(getExecutor(), clientStorage.resolve(FILE)))
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals("test content".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadSuffix() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE, 13))
				.streamTo(ChannelFileWriter.open(getExecutor(), clientStorage.resolve(FILE)))
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals("of the file".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadPart() throws IOException {
		await(ChannelSupplier.ofPromise(client.download(FILE, 5, 10))
				.streamTo(ChannelFileWriter.open(getExecutor(), clientStorage.resolve(FILE)))
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals("content of".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadOverSuffix() throws IOException {
		Throwable exception = awaitException(ChannelSupplier.ofPromise(client.download(FILE, 13, 123))
				.streamTo(ChannelFileWriter.open(getExecutor(), clientStorage.resolve(FILE)))
				.whenComplete(($, e) -> server.close()));

		assertSame(LENGTH_TOO_BIG, exception);
	}

	@Test
	public void downloadOver() throws IOException {
		Throwable exception = awaitException(ChannelSupplier.ofPromise(client.download(FILE, 123, 123))
				.streamTo(ChannelFileWriter.open(getExecutor(), clientStorage.resolve(FILE)))
				.whenComplete(($, e) -> server.close()));

		assertSame(OFFSET_TOO_BIG, exception);
	}

	@Test
	public void overridingUpload() throws IOException {
		Path path = serverStorage.resolve("test_file.txt");
		String content = "Hello! Ima slow green fox, running over an active dog";
		String override = "over an active dog, that is the best dog ever possible in existense";
		String updated = "Hello! Ima slow green fox, running over an active dog, that is the best dog ever possible in existense";

		Files.write(path, content.getBytes(UTF_8));

		await(ChannelSupplier.of(ByteBuf.wrapForReading(override.getBytes(UTF_8)))
				.streamTo(ChannelConsumer.ofPromise(client.upload(path.getFileName().toString(), 35)))
				.whenComplete(($, e) -> server.close()));

		assertArrayEquals(updated.getBytes(UTF_8), Files.readAllBytes(path));
	}
}
