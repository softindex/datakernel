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

package io.datakernel.hashfs2;

import com.google.common.collect.Sets;
import io.datakernel.async.CompletionCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioService;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.fail;

public class IntegrationSingleNodeTest {
	private static final Logger logger = LoggerFactory.getLogger(IntegrationSingleNodeTest.class);

	private final ServerInfo local = new ServerInfo(0, new InetSocketAddress("127.0.0.1", 4455), 1.0);
	private final Config config = Config.getDefaultConfig();

	private String serverStorage = "server_storage";
	private String clientStorage = "client_storage";

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() {

	}

	@Test
	public void testUpload() {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = newCachedThreadPool();
		final NioService server = ServerFactory.getServer(eventloop, executor, serverStorage, config, local, Sets.newHashSet(local));
		FsClient client = ServerFactory.getClient(eventloop, Sets.newHashSet(local), config);
		StreamProducer<ByteBuf> producer = StreamFileReader.readFileFully(eventloop, executor, 10 * 256, Paths.get("./test/client_storage/rejected.txt"));

		server.start(ignoreCompletionCallback());
		client.upload("rejected.txt", producer, new CompletionCallback() {
			@Override
			public void onComplete() {
				server.stop(new CompletionCallback() {
					@Override
					public void onComplete() {
						System.out.println("Stopped");
					}

					@Override
					public void onException(Exception e) {
						System.out.println("Failed to stop");
					}
				});
				System.out.println("Uploaded");
			}

			@Override
			public void onException(Exception exception) {
				System.out.println("Failed");
			}
		});
		eventloop.run();
		executor.shutdownNow();
	}

	@Test
	public void testDownload() {
		fail("Not yet implemented");
	}
}
