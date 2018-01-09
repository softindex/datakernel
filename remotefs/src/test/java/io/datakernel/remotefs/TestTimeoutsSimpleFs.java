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

package io.datakernel.remotefs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.async.CallbackRegistry;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.remotefs.FsIntegrationTest.createBigByteArray;
import static io.datakernel.stream.DataStreams.stream;

public class TestTimeoutsSimpleFs {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path storagePath;
	private byte[] BIG_FILE = createBigByteArray();

	@Before
	public void setUp() throws IOException {
		storagePath = Paths.get(temporaryFolder.newFolder("server_storage").toURI());
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	@Ignore
	public void testUploadTimeout() throws ExecutionException, InterruptedException, IOException {
		CallbackRegistry.setStoreStackTrace(true);
		((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);

		InetSocketAddress address = new InetSocketAddress("localhost", 7010);
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		RemoteFsClient client = RemoteFsClient.create(eventloop, address);

		ExecutorService serverExecutor = Executors.newFixedThreadPool(2);
		RemoteFsServer server = RemoteFsServer.create(eventloop, serverExecutor, storagePath)
				.withSocketSettings(SocketSettings.create().withImplReadTimeout(1L))
				.withAcceptOnce()
				.withListenAddress(new InetSocketAddress("localhost", 7010));

		server.listen();

		StreamProducer<ByteBuf> producer = StreamProducers.of(ByteBuf.wrapForReading(BIG_FILE));
		StreamConsumerWithResult<ByteBuf, Void> consumer = client.uploadStream("fileName.txt");
		stream(producer, consumer);
		CompletableFuture<Void> future = consumer.getResult().toCompletableFuture();

		eventloop.run();

//		thrown.expect(ExecutionException.class);
		future.get();
	}

}
