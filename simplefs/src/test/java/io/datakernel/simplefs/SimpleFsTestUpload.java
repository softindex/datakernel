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

import com.google.common.base.Charsets;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SimpleNioServer;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class SimpleFsTestUpload {
	private static final int LISTEN_PORT = 6432;
	private static final InetSocketAddress address = new InetSocketAddress("127.0.0.1", LISTEN_PORT);
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	private Path dirPath;

	@Before
	public void before() throws Exception {
		dirPath = Paths.get(folder.newFolder("test_data").toURI());

		Files.createDirectories(dirPath);

		Path t1 = dirPath.resolve("t1");
		Files.write(t1, ("some text\n\nmore text\t\n\n\r").getBytes(Charsets.UTF_8));

		Path t2 = dirPath.resolve("t2");
		Files.write(t2, ("\n\raaa\nbbb").getBytes(Charsets.UTF_8));

		Path t3 = dirPath.resolve("empty_file");
		Files.createFile(t3);

		Path t4 = dirPath.resolve("a b");
		Files.write(t4, ("a\nb\nc").getBytes(Charsets.UTF_8));

		Path t5 = dirPath.resolve("big_file");

		StringBuilder testFileContentBuilder = new StringBuilder();
		for (int i = 0; i < 100000; i++) {
			testFileContentBuilder.append(i);
			testFileContentBuilder.append("\n");
		}
		Files.write(t5, testFileContentBuilder.toString().getBytes(Charsets.UTF_8));
	}

	@Test
	public void testUpload() throws Exception {
		final NioEventloop eventloop = new NioEventloop();

		final ExecutorService executor = Executors.newCachedThreadPool();

		SimpleNioServer server = new SimpleNioServer(eventloop) {
			@Override
			protected SocketConnection createConnection(SocketChannel socketChannel) {
				return new StreamMessagingConnection<>(eventloop, socketChannel,
						new StreamGsonDeserializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 16 * 1024),
						new StreamGsonSerializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 16 * 1024, 1024, 0))
						.addHandler(SimpleFsCommandUpload.class, new MessagingHandler<SimpleFsCommandUpload, SimpleFsResponse>() {
							@Override
							public void onMessage(SimpleFsCommandUpload item, Messaging<SimpleFsResponse> messaging) {
								Path destination = dirPath.resolve(item.filename);
								StreamConsumer<ByteBuf> diskWrite = StreamFileWriter.createFile(eventloop, executor, destination, true);
								messaging.binarySocketReader().streamTo(diskWrite);
								messaging.shutdownWriter();
							}
						});
			}
		};

		server.setListenAddress(address).acceptOnce();
		server.listen();

		eventloop.connect(address, new SocketSettings(), new ConnectCallback() {
					@Override
					public void onConnect(SocketChannel socketChannel) {
						SocketConnection connection = new StreamMessagingConnection<>(eventloop, socketChannel,
								new StreamGsonDeserializer<>(eventloop, SimpleFsResponseSerialization.GSON, SimpleFsResponse.class, 16 * 1024),
								new StreamGsonSerializer<>(eventloop, SimpleFsCommandSerialization.GSON, SimpleFsCommand.class, 16 * 1024, 1024, 0))
								.addStarter(new MessagingStarter<SimpleFsCommand>() {
									@Override
									public void onStart(Messaging<SimpleFsCommand> messaging) {

										SimpleFsCommandUpload commandUpload = new SimpleFsCommandUpload("big_file_uploaded");
										messaging.sendMessage(commandUpload);

										final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor,
												16 * 1024, dirPath.resolve("big_file"));
										producer.streamTo(messaging.binarySocketWriter());
										messaging.shutdownReader();
									}
								});
						connection.register();
					}

					@Override
					public void onException(Exception exception) {
						fail("Test Exception: " + exception);
					}
				}
		);

		eventloop.run();
		assertTrue(com.google.common.io.Files.equal(dirPath.resolve("big_file_uploaded").toFile(), dirPath.resolve("big_file").toFile()));
	}

}
