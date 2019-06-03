package io.global.chat;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.messages.MessageOperation;
import io.global.common.SimKey;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.server.GlobalOTNodeImpl;
import io.global.ot.service.ServiceEnsuringServlet;
import io.global.ot.service.messaging.MessagingServlet;
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
		bind(new TypeLiteral<OTSystem<MessageOperation>>() {}).toInstance(createOTSystem());
		bind(new TypeLiteral<StructuredCodec<MessageOperation>>() {}).toInstance(MESSAGE_OPERATION_CODEC);
		super.configure();
	}

	@Provides
	@Singleton
	@Named("Chat")
	AsyncHttpServer provideServer(Eventloop eventloop, ServiceEnsuringServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	MiddlewareServlet provideMainServlet(
			DynamicOTNodeServlet<ContactsOperation> contactsServlet,
			DynamicOTNodeServlet<SharedReposOperation> roomListServlet,
			DynamicOTNodeServlet<MessageOperation> roomServlet,
			MessagingServlet messagingServlet,
			StaticServlet staticServlet
	) {
		return MiddlewareServlet.create()
				.with("/contacts", contactsServlet)
				.with("/index", roomListServlet)
				.with("/room/:suffix", roomServlet)
				.with("/rooms", messagingServlet)
				.withFallback(staticServlet);
	}

	@Provides
	@Singleton
	StaticServlet provideStaticServlet(Eventloop eventloop, Executor executor) {
		Path staticDir = Paths.get(RESOURCES_PATH);
		StaticLoader resourceLoader = StaticLoaders.ofPath(executor, staticDir);
		return StaticServlet.create(eventloop, resourceLoader, "index.html");
	}

	@Provides
	@Singleton
	OTDriver provideDriver(Eventloop eventloop, GlobalOTNodeImpl node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		return new OTDriver(node, simKey);
	}

	@Provides
	@Singleton
	Executor provideExecutor(Config config) {
		return getExecutor(config.getChild("executor"));
	}

}
