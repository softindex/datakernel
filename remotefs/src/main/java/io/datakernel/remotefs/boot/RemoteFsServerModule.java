package io.datakernel.remotefs.boot;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.util.guice.SimpleModule;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.ConfigConverters.ofAbstractServerInitializer;
import static io.datakernel.config.ConfigConverters.ofPath;

public class RemoteFsServerModule extends SimpleModule {
	public static final Path DEFAULT_STORAGE = Paths.get("storage");

	// region creators
	private RemoteFsServerModule() {
	}

	public static RemoteFsServerModule create() {
		return new RemoteFsServerModule();
	}
	// endregion

	@Provides
	@Singleton
	RemoteFsServer provideServer(Eventloop eventloop, ExecutorService executor, Config config) {
		return RemoteFsServer.create(eventloop, executor, config.get(ofPath(), "remotefs.server.storage", DEFAULT_STORAGE))
				.initialize(config.get(ofAbstractServerInitializer(new InetSocketAddress(8080)), "remotefs.server"));
	}
}
