package io.datakernel.simplefs;

import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
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

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.simplefs.SimpleFsIntegrationTest.createBigByteArray;
import static org.junit.Assert.assertThat;

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
	public void testUploadTimeout() throws ExecutionException, InterruptedException, IOException {
		InetSocketAddress address = new InetSocketAddress(7010);
		Eventloop eventloop = Eventloop.create();
		SimpleFsClient client = SimpleFsClient.create(eventloop, address);

		final ExecutorService serverExecutor = Executors.newFixedThreadPool(2);
		final SimpleFsServer server = SimpleFsServer.create(eventloop, serverExecutor, storagePath)
				.withSocketSettings(SocketSettings.defaultSocketSettings().withReadTimeout(1L))
				.withAcceptOnce()
				.withListenPort(7010);

		server.listen();

		CompletionCallbackFuture callback = CompletionCallbackFuture.create();

		client.upload("fileName.txt", StreamProducers.ofValue(eventloop, ByteBuf.wrapForReading(BIG_FILE)), callback);

		eventloop.run();

//		thrown.expect(ExecutionException.class);
		callback.get();
		assertThat(eventloop, doesntHaveFatals());
	}

}
