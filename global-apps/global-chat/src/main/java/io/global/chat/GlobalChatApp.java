package io.global.chat;

import io.datakernel.async.Promise;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.chat.chatroom.ChatRoomOTSystem;
import io.global.chat.chatroom.operation.ChatRoomOperation;
import io.global.common.BinaryDataFormats;
import io.global.common.PrivKey;
import io.global.kv.api.GlobalKvNode;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.MapModule;
import io.global.ot.OTAppCommonModule;
import io.global.ot.SharedRepoModule;
import io.global.ot.contactlist.ContactsModule;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.UserContainerModule;
import io.global.ot.shared.IndexRepoModule;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.PmClient;
import io.global.pm.http.PmClientServlet;

import java.util.Comparator;
import java.util.concurrent.CompletionStage;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.override;
import static io.global.chat.Utils.CHAT_ROOM_OPERATION_CODEC;

public final class GlobalChatApp extends Launcher {
	private static final String PROPERTIES_FILE = "chat.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Chat";
	private static final String CHAT_REPO_PREFIX = "chat/room";
	private static final String CHAT_INDEX_REPO = "chat/index";
	private static final String PROFILE_REPO_NAME = "profile";

	@Inject
	AsyncHttpServer server;

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
				.with("node.serverId", DEFAULT_SERVER_ID)
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	<T extends Comparable<? super T>> Comparator<T> comparator() {
		return Comparator.naturalOrder();
	}

	@Provides
	OTSystem<ChatRoomOperation> chatRoomOTSystem() {
		return ChatRoomOTSystem.createOTSystem();
	}

	@Provides
	CodecFactory codecFactory() {
		return BinaryDataFormats.createGlobal()
				.with(ChatRoomOperation.class, CHAT_ROOM_OPERATION_CODEC);
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
					return Promise.ofException(new ParseException(GlobalChatApp.class, "Cookie `Key` is required"));
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

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new OTAppCommonModule(),
				new ContactsModule(),
				new MapModule<String, String>(PROFILE_REPO_NAME) {},
				new IndexRepoModule(CHAT_INDEX_REPO),
				new UserContainerModule<ChatRoomOperation>(CHAT_INDEX_REPO, CHAT_REPO_PREFIX) {},
				new SharedRepoModule<ChatRoomOperation>(CHAT_REPO_PREFIX) {},
				// override for debug purposes
				override(new GlobalNodesModule(),
						new LocalNodeCommonModule(DEFAULT_SERVER_ID))
		);
	}

	@Override
	public void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalChatApp().launch(args);
	}
}
