package io.datakernel.remotefs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.datakernel.async.CallbackRegistry;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
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
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		RemoteFsClient client = RemoteFsClient.create(eventloop, address);

		final ExecutorService serverExecutor = Executors.newFixedThreadPool(2);
		final RemoteFsServer server = RemoteFsServer.create(eventloop, serverExecutor, storagePath)
				.withSocketSettings(SocketSettings.create().withImplReadTimeout(1L))
				.withAcceptOnce()
				.withListenAddress(new InetSocketAddress("localhost", 7010));

		server.listen();

		final StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(BIG_FILE));
		final CompletableFuture<Void> future = client.upload(producer, "fileName.txt").toCompletableFuture();

		eventloop.run();

//		thrown.expect(ExecutionException.class);
		future.get();
	}

}
