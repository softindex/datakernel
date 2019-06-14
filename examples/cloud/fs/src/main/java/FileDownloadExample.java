import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.UncheckedException;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.service.ServiceGraphModule;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.logging.Level.SEVERE;

/**
 * This example demonstrates downloading file from RemoteFS server.
 * To run this example you should first launch ServerSetupExample and then FileUploadExample
 */
public final class FileDownloadExample extends Launcher {
	private static final int SERVER_PORT;
	private static final Path CLIENT_STORAGE;
	private static final String REQUIRED_FILE;
	private static final String DOWNLOADED_FILE;

	static {
		SERVER_PORT = 6732;
		REQUIRED_FILE = "example.txt";
		DOWNLOADED_FILE = "downloaded_example.txt";
		try {
			CLIENT_STORAGE = Files.createTempDirectory("client_storage");
		} catch (IOException e) {
			throw new UncheckedException(e);
		}
	}
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
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
		return ServiceGraphModule.defaultInstance();
	}

	@Override
	protected void run() throws Exception {
		eventloop.post(() -> {
			// producer result here means that file was successfully downloaded from server
			ChannelSupplier.ofPromise(client.download(REQUIRED_FILE, 0))
					.streamTo(ChannelFileWriter.create(CLIENT_STORAGE.resolve(DOWNLOADED_FILE)))
					.whenComplete(($, e) -> {
						if (e != null) logger.log(SEVERE, "Download is failed", e);
						shutdown();
					});
		});
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		FileDownloadExample example = new FileDownloadExample();
		example.launch(args);
	}
}
