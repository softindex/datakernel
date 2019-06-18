import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.MemSize;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This example demonstrates uploading file to server using RemoteFS
 * To run this example you should first launch ServerSetupExample
 */
public final class FileUploadExample extends Launcher {
	private static final String FILE_NAME = "example.txt";
	private static final int SERVER_PORT = 6732;;
	private Path clientFile;

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
	protected void onStart() throws Exception {
		clientFile = Files.createTempFile("example", ".txt");
		Files.write(clientFile, "example text".getBytes());
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.defaultInstance();
	}

	@Override
	protected void run() throws Exception {
		eventloop.post(() -> {
			// consumer result here is a marker of it being successfully uploaded
			ChannelFileReader.readFile(clientFile)
					.then(cfr -> cfr.withBufferSize(MemSize.kilobytes(16)).streamTo(client.upload(FILE_NAME)))
					.whenComplete(($, e) -> {
						if (e != null) logger.error("Upload failed", e);
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
