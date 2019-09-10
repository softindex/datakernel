package io.global.chat;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.messages.MessageOperation;
import io.global.common.SimKey;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ServiceEnsuringServlet;
import io.global.ot.shared.SharedReposOperation;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.chat.Utils.MESSAGE_OPERATION_CODEC;
import static io.global.chat.chatroom.messages.MessagesOTSystem.createOTSystem;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class ChatModule extends AbstractModule {
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final String RESOURCES_PATH = "front/build";

	@Override
	protected void configure() {
		bind(new Key<OTSystem<MessageOperation>>() {}).toInstance(createOTSystem());
		bind(new Key<StructuredCodec<MessageOperation>>() {}).toInstance(MESSAGE_OPERATION_CODEC);
		super.configure();
	}

	@Provides
	@Named("Chat")
	AsyncHttpServer provideServer(Eventloop eventloop, ServiceEnsuringServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	RoutingServlet provideMainServlet(
			DynamicOTNodeServlet<ContactsOperation> contactsServlet,
			DynamicOTNodeServlet<SharedReposOperation> roomListServlet,
			DynamicOTNodeServlet<MessageOperation> roomServlet,
			DynamicOTNodeServlet<MapOperation<String, String>> profileServlet,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/contacts/*", contactsServlet)
				.map("/ot/rooms/*", roomListServlet)
				.map("/ot/room/:suffix/*", roomServlet)
				.map("/ot/profile/:pubKey/*", profileServlet)
				.map("/ot/myProfile/*", profileServlet)
				.map("/*", staticServlet);
	}

	@Provides
	StaticServlet provideStaticServlet(Eventloop eventloop, Executor executor) {
		Path staticDir = Paths.get(RESOURCES_PATH);
		StaticLoader resourceLoader = StaticLoader.ofPath(executor, staticDir);
		return StaticServlet.create(resourceLoader)
				.withMappingNotFoundTo("index.html");
	}

	@Provides
	OTDriver provideDriver(Eventloop eventloop, GlobalOTNode node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	Executor provideExecutor(Config config) {
		return getExecutor(config.getChild("executor"));
	}

}
