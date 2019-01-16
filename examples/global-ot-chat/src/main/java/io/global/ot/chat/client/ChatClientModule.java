package io.global.ot.chat.client;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.global.ot.chat.common.Gateway;
import io.global.ot.chat.http.GatewayHttpClient;
import io.global.ot.chat.operations.ChatOperation;

import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.ot.chat.operations.ChatOperation.OPERATION_CODEC;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class ChatClientModule extends AbstractModule {

	@Provides
	@Singleton
	Eventloop provideEventloop(Config config) {
		return Eventloop.create()
				.initialize(ofEventloop(config));
	}

	@Provides
	@Singleton
	AsyncHttpServer provideServer(Eventloop eventloop, MiddlewareServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	MiddlewareServlet provideMiddlewareServlet(ClientServlet apiServlet, StaticServlet staticServlet, @Named("Graph") AsyncServlet graphServlet) {
		return MiddlewareServlet.create()
				.with("/api", apiServlet)
				.with(GET, "/graph", graphServlet)
				.withFallback(staticServlet);
	}

	@Provides
	@Singleton
	StaticServlet provideStaticServlet(Eventloop eventloop, Config config) {
		StaticLoader resourceLoader = StaticLoaders.ofPath(newCachedThreadPool(), config.get(ofPath(), "resources.path"));
		return StaticServlet.create(eventloop, resourceLoader);
	}

	@Provides
	@Singleton
	ChatStateManager provideStateManager(Eventloop eventloop, Gateway<ChatOperation> gateway) {
		return ChatStateManager.create(eventloop, gateway);
	}

	@Provides
	@Singleton
	ClientServlet provideClientServlet(ChatStateManager stateManager) {
		return ClientServlet.create(stateManager);
	}

	@Provides
	@Singleton
	Gateway<ChatOperation> provideGateway(Eventloop eventloop, Config config) {
		return GatewayHttpClient.create(AsyncHttpClient.create(eventloop), config.get("gateway.url"), OPERATION_CODEC);
	}
}
