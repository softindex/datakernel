package io.global.chat;

import io.datakernel.async.Promise;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.datakernel.ot.OTSystem;
import io.global.chat.chatroom.operation.ChatRoomOperation;
import io.global.common.PrivKey;
import io.global.common.SimKey;
import io.global.kv.api.GlobalKvNode;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.api.GlobalOTNode;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ServiceEnsuringServlet;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.PmClient;
import io.global.pm.http.PmClientServlet;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.ConfigConverters.getExecutor;
import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.chat.Utils.CHAT_ROOM_OPERATION_CODEC;
import static io.global.chat.chatroom.ChatRoomOTSystem.createOTSystem;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class ChatModule extends AbstractModule {
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});
	public static final String RESOURCES_PATH = "front/build";

	@Override
	protected void configure() {
		bind(new Key<OTSystem<ChatRoomOperation>>() {}).toInstance(createOTSystem());
		bind(new Key<StructuredCodec<ChatRoomOperation>>() {}).toInstance(CHAT_ROOM_OPERATION_CODEC);
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
			DynamicOTNodeServlet<ChatRoomOperation> roomServlet,
			DynamicOTNodeServlet<MapOperation<String, String>> profileServlet,
			@Named("Calls") AsyncServlet callsServlet,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/contacts/*", contactsServlet)
				.map("/ot/rooms/*", roomListServlet)
				.map("/ot/room/:suffix/*", roomServlet)
				.map("/ot/profile/:pubKey/*", profileServlet)
				.map("/ot/myProfile/*", profileServlet)
				.map("/calls/*", callsServlet)
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

	@Provides
	GlobalPmDriver<String> callsPMDriver(GlobalKvNode node) {
		return new GlobalPmDriver<>(node, STRING_CODEC);
	}

	@Provides
	@Named("Calls")
	AsyncServlet callsPMServlet(GlobalPmDriver<String> driver) {
		return request -> {
			try {
				String key = request.getCookie("Key");
				if (key == null) {
					return Promise.ofException(new ParseException(ChatModule.class, "Cookie `Key` is required"));
				}
				PrivKey privKey = PrivKey.fromString(key);
				PmClient<String> client = driver.adapt(privKey.computeKeys());
				return PmClientServlet.create(client, STRING_CODEC)
						.serve(request);
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

}
