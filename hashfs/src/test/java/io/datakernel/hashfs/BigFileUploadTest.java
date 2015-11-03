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

import com.google.common.base.Charsets;
import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.stub.upload_big.Client;
import io.datakernel.hashfs.stub.upload_big.GsonServer;
import io.datakernel.hashfs.stub.upload_big.Server;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.file.StreamFileReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.TestCase.assertTrue;

public class BigFileUploadTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testUploadFile() throws IOException {

		final NioEventloop eventloop = new NioEventloop();
		final ExecutorService executor = Executors.newCachedThreadPool();

		final InetSocketAddress address = new InetSocketAddress("127.0.0.1", 5628);

		Server server = new Server(eventloop, executor);
		final GsonServer gsonServer = GsonServer.createServerTransport(address, eventloop, server);

		Path source = folder.newFile("big_file").toPath();
		Path destination = folder.newFile("big_file_uploaded").toPath();

		StringBuilder testFileContentBuilder = new StringBuilder();
		for (int i = 0; i < 10000000; i++) {
			testFileContentBuilder.append(i);
			testFileContentBuilder.append("\n");
		}
		Files.write(source, testFileContentBuilder.toString().getBytes(Charsets.UTF_8));

		Client client = Client.createHashClient(eventloop);

		StreamConsumer<ByteBuf> consumer = client.upload(address, destination.toAbsolutePath().toString());
		final StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 16 * 1024, source);
		producer.streamTo(consumer);
//
//		consumer.addCompletionCallback(new CompletionCallback() {
//			@Override
//			public void onComplete() {
//				gsonServer.close();
//			}
//
//			@Override
//			public void onException(Exception exception) {
//				gsonServer.close();
//			}
//		});

		eventloop.run();
		assertTrue(com.google.common.io.Files.equal(source.toFile(), destination.toFile()));
	}

}
