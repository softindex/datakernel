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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.*;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.chat.chatroom.operation.ChatRoomOperation;
import io.global.common.PrivKey;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.GlobalKvNode;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.*;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsModule;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.CommonUserContainer;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.session.AuthModule;
import io.global.ot.session.KvSessionStore;
import io.global.ot.session.UserId;
import io.global.ot.shared.IndexRepoModule;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.Messenger;
import io.global.pm.MessengerServlet;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.global.chat.Utils.CHAT_ROOM_OPERATION_CODEC;
import static io.global.chat.Utils.CHAT_ROOM_OT_SYSTEM;
import static io.global.common.CryptoUtils.randomBytes;
import static io.global.common.CryptoUtils.toHexString;
import static io.global.ot.OTUtils.SHARED_REPO_MESSAGE_CODEC;

public final class GlobalChatApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-chat.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Chat";
	private static final String CHAT_REPO_PREFIX = "chat/room";
	private static final String CHAT_INDEX_REPO = "chat/index";
	private static final String CHAT_SESSION_TABLE = "chat/session";
	private static final String PROFILE_REPO_NAME = "profile";
	private static final String SESSION_ID = "CHAT_SID";
	private static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");

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
	OTSystem<ChatRoomOperation> chatRoomOTSystem() {
		return CHAT_ROOM_OT_SYSTEM;
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
				.map("/*", staticServlet)
				.merge(authorizationServlet);
	}

	@Provides
	Messenger<Long, CreateSharedRepo> messenger(GlobalKvNode node) {
		Random random = new Random();
		return Messenger.create(node, LONG_CODEC, SHARED_REPO_MESSAGE_CODEC, random::nextLong);
	}

	@Provides
	Messenger<String, String> notificationsMessenger(GlobalKvNode node) {
		return Messenger.create(node, STRING_CODEC, STRING_CODEC, () -> toHexString(randomBytes(8)));
	}

	@Provides
	@ContainerScope
	CommonUserContainer<ChatRoomOperation> userContainer(Eventloop eventloop, PrivKey privKey, OTDriver driver, GlobalKvDriver<String, UserId> kvDriver, Messenger<Long, CreateSharedRepo> messenger) {
		RepoID repoID = RepoID.of(privKey, CHAT_REPO_PREFIX);
		MyRepositoryId<ChatRoomOperation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, CHAT_ROOM_OPERATION_CODEC);
		KvSessionStore<UserId> sessionStore = KvSessionStore.create(eventloop, kvDriver.adapt(privKey), CHAT_SESSION_TABLE);
		return CommonUserContainer.create(eventloop, driver, CHAT_ROOM_OT_SYSTEM, myRepositoryId, messenger, sessionStore, CHAT_INDEX_REPO);
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new OTAppCommonModule(),
				new AuthModule<CommonUserContainer<ChatRoomOperation>>(SESSION_ID) {},
				new ContactsModule(),
				new MapModule<String, String>(PROFILE_REPO_NAME) {},
				new IndexRepoModule(CHAT_INDEX_REPO),
				new ContainerModule<CommonUserContainer<ChatRoomOperation>>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
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
