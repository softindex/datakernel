import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * This example demonstrates downloading file from RemoteFS server.
 * To run this example you should first launch ServerSetupExample and then FileUploadExample
 */
public final class FileDownloadExample extends Launcher {
	private static final int SERVER_PORT = 6732;
	private static final String REQUIRED_FILE = "example.txt";
	private static final String DOWNLOADED_FILE = "downloaded_example.txt";

	private Path clientStorage;

	@Override
	protected void onInit(Injector injector) throws Exception {
		clientStorage = Files.createTempDirectory("client_storage");
	}

	@Inject
	private RemoteFsClient client;

	@Inject
	private Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RemoteFsClient remoteFsClient(Eventloop eventloop) {
		return RemoteFsClient.create(eventloop, new InetSocketAddress(SERVER_PORT));
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws Exception {
		CompletableFuture<Void> future = eventloop.submit(() ->
				ChannelSupplier.ofPromise(client.download(REQUIRED_FILE, 0))
						.streamTo(ChannelFileWriter.open(newSingleThreadExecutor(), clientStorage.resolve(DOWNLOADED_FILE)))
		);
		future.get();
	}

	public static void main(String[] args) throws Exception {
		FileDownloadExample example = new FileDownloadExample();
		example.launch(args);
	}
}
