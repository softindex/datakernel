import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.UncheckedException;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.MemSize;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.logging.Level.SEVERE;

/**
 * This example demonstrates uploading file to server using RemoteFS
 * To run this example you should first launch ServerSetupExample
 */
public final class FileUploadExample extends Launcher {
	private static final int SERVER_PORT;
	private static final Path CLIENT_FILE;
	private static final String FILE_NAME;

	static {
		SERVER_PORT = 6732;
		FILE_NAME = "example.txt";
		try {
			CLIENT_FILE = Files.createTempFile("example", ".txt");
			Files.write(CLIENT_FILE, "example text".getBytes());
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
			// consumer result here is a marker of it being successfully uploaded
			ChannelFileReader.readFile(CLIENT_FILE)
					.then(cfr -> cfr.withBufferSize(MemSize.kilobytes(16)).streamTo(client.upload(FILE_NAME)))
					.whenComplete(($, e) -> {
						if (e != null) logger.log(SEVERE, "Upload failed", e);
						shutdown();
					});
		});
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		FileUploadExample example = new FileUploadExample();
		example.launch(args);
	}
}
