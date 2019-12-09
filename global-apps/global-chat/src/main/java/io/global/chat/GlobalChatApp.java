package io.global.chat;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.chat.chatroom.operation.ChatRoomOperation;
import io.global.kv.api.GlobalKvNode;
import io.global.kv.api.KvClient;
import io.global.launchers.GlobalNodesModule;
import io.global.launchers.sync.KvSyncModule;
import io.global.launchers.sync.OTSyncModule;
import io.global.ot.*;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.SharedUserContainer;
import io.global.ot.session.AuthModule;
import io.global.ot.session.UserId;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.Messenger;
import io.global.pm.MessengerServlet;
import io.global.session.KvSessionModule;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.global.Utils.DEFAULT_SYNC_SCHEDULE_CONFIG;
import static io.global.Utils.cachedContent;
import static io.global.chat.Utils.CHAT_ROOM_OPERATION_CODEC;
import static io.global.chat.Utils.CHAT_ROOM_OT_SYSTEM;
import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;

public final class GlobalChatApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-chat.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Chat";
	private static final String SESSION_ID = "CHAT_SID";
	private static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");

	@Inject
	AsyncHttpServer server;

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("kv.catchUp.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.with("kv.push.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.with("ot.update.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	TypedRepoNames typedRepoNames() {
		return TypedRepoNames.create("global-chat")
				.withRepoName(Key.of(SharedReposOperation.class), "index")
				.withGlobalRepoName(new Key<MapOperation<String, String>>() {}, "profile")
				.withGlobalRepoName(new Key<ContactsOperation>() {}, "contacts")
				.withRepoPrefix(Key.of(ChatRoomOperation.class), "room")
				.withRepoName(new Key<KvClient<String, UserId>>() {}, "session");
	}

	@Provides
	CodecFactory codecFactory() {
		return OTUtils.createOTRegistry()
				.with(ChatRoomOperation.class, CHAT_ROOM_OPERATION_CODEC);
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTUplinkServlet<ContactsOperation> contactsServlet,
			DynamicOTUplinkServlet<SharedReposOperation> roomListServlet,
			DynamicOTUplinkServlet<ChatRoomOperation> roomServlet,
			DynamicOTUplinkServlet<MapOperation<String, String>> profileServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			Messenger<String, String> notificationsMessenger,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/*", sessionDecorator.serve(RoutingServlet.create()
						.map("/contacts/*", contactsServlet)
						.map("/rooms/*", roomListServlet)
						.map("/room/:suffix/*", roomServlet)
						.map("/profile/:key/*", profileServlet)
						.map("/myProfile/*", profileServlet)))
				.map("/notifications/*", sessionDecorator.serve(MessengerServlet.create(notificationsMessenger)))
				.map("/static/*", cachedContent().serve(staticServlet))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
	}

	@Provides
	Messenger<String, String> notificationsMessenger(GlobalKvNode node) {
		return Messenger.create(node, STRING_CODEC, STRING_CODEC, () -> toHexString(randomBytes(8)));
	}

	@Provides
	@Named("repo prefix")
	String repoPrefix(TypedRepoNames names) {
		return names.getRepoPrefix(Key.of(ChatRoomOperation.class));
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new OTAppCommonModule(),
				new AuthModule<SharedUserContainer<ChatRoomOperation>>(SESSION_ID) {},
				OTGeneratorsModule.create(),
				KvSessionModule.create(),
				new ContainerModule<SharedUserContainer<ChatRoomOperation>>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new SharedRepoModule<ChatRoomOperation>() {
					@Override
					protected void configure() {
						bind(new Key<SharedUserContainer<ChatRoomOperation>>() {}).in(ContainerScope.class);
						bind(Key.of(String.class).named("mail box")).toInstance("global-chat");
						bind(new Key<OTSystem<ChatRoomOperation>>() {}).toInstance(CHAT_ROOM_OT_SYSTEM);
						super.configure();
					}
				},
				override(new GlobalNodesModule(),
						new LocalNodeCommonModule(DEFAULT_SERVER_ID)),
				new KvSyncModule(),
				new OTSyncModule()
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
