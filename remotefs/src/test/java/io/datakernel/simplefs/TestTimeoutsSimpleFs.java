package io.datakernel.simplefs;

import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.RunnableWithException;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.simplefs.SimpleFsIntegrationTest.createBigByteArray;

public class TestTimeoutsSimpleFs {
	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path storagePath;
	private byte[] BIG_FILE = createBigByteArray();

	private ExecutorService executorService = Executors.newCachedThreadPool();

	@Before
	public void setUp() throws IOException {
		storagePath = Paths.get(temporaryFolder.newFolder("server_storage").toURI());
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testUploadTimeout() throws ExecutionException, InterruptedException {
		InetSocketAddress address = new InetSocketAddress(7000);
		Eventloop eventloop = new Eventloop();
		SimpleFsClient client = new SimpleFsClient(eventloop, address);

		startAcceptOnceServer();

		CompletionCallbackFuture callback = new CompletionCallbackFuture();

		client.upload("fileName.txt", StreamProducers.ofValue(eventloop, ByteBuf.wrap(BIG_FILE)), callback);

		eventloop.run();
		executorService.shutdown();

		thrown.expect(ExecutionException.class);
		callback.get();
	}

	private void startAcceptOnceServer() {
		final Eventloop serverEventloop = new Eventloop();
		final ExecutorService serverExecutor = Executors.newFixedThreadPool(2);
		final SimpleFsServer server = new SimpleFsServer(serverEventloop, serverExecutor, storagePath)
				.socketSettings(SocketSettings.defaultSocketSettings().readTimeout(1L).writeTimeout(1L))
				.acceptOnce()
				.setListenPort(7000);

		executorService.submit(new RunnableWithException() {
			@Override
			public void runWithException() throws Exception {
				server.listen();
				serverEventloop.run();
				serverExecutor.shutdown();
			}
		});
	}
}
