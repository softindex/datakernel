package io.global.chat;

import io.datakernel.async.Promise;
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
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.chat.chatroom.operation.ChatRoomOperation;
import io.global.common.BinaryDataFormats;
import io.global.common.PrivKey;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.MapModule;
import io.global.ot.OTAppCommonModule;
import io.global.ot.SharedRepoModule;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsModule;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.CommonUserContainer;
import io.global.ot.service.ContainerManager;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.shared.IndexRepoModule;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.GlobalPmDriver;
import io.global.pm.api.GlobalPmNode;
import io.global.pm.http.GlobalPmDriverServlet;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.util.CollectionUtils.concat;
import static io.global.chat.Utils.*;
import static io.global.ot.OTUtils.SHARED_REPO_MESSAGE_CODEC;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

public final class GlobalChatApp extends Launcher {
	private static final String PROPERTIES_FILE = "chat.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Chat";
	private static final String CHAT_REPO_PREFIX = "chat/room";
	private static final String CHAT_INDEX_REPO = "chat/index";
	private static final String PROFILE_REPO_NAME = "profile";
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
	<T extends Comparable<? super T>> Comparator<T> comparator() {
		return Comparator.naturalOrder();
	}

	@Provides
	OTSystem<ChatRoomOperation> chatRoomOTSystem() {
		return CHAT_ROOM_OT_SYSTEM;
	}

	@Provides
	CodecFactory codecFactory() {
		return BinaryDataFormats.createGlobal()
				.with(ChatRoomOperation.class, CHAT_ROOM_OPERATION_CODEC);
	}

	@Provides
	AsyncServlet provideMainServlet(
			DynamicOTNodeServlet<ContactsOperation> contactsServlet,
			DynamicOTNodeServlet<SharedReposOperation> roomListServlet,
			DynamicOTNodeServlet<ChatRoomOperation> roomServlet,
			DynamicOTNodeServlet<MapOperation<String, String>> profileServlet,
			@Named("Calls") AsyncServlet callsServlet,
			StaticServlet staticServlet,
			Executor executor,
			ContainerManager<CommonUserContainer<ChatRoomOperation>> containerManager
	) {
		Path expectedKeys = DEFAULT_CONTAINERS_DIR.resolve("expected.dat");
		return RoutingServlet.create()
				.map("/ot/contacts/*", contactsServlet)
				.map("/ot/rooms/*", roomListServlet)
				.map("/ot/room/:suffix/*", roomServlet)
				.map("/ot/profile/:pubKey/*", profileServlet)
				.map("/ot/myProfile/*", profileServlet)
				.map("/notifications/*", callsServlet)
				.map("/*", staticServlet)

				// for backwards compatibility, to be removed later
				.then(servlet -> request -> {
					String key = request.getCookie("Key");
					if (key == null) {
						return servlet.serve(request);
					} else {
						return Promise.ofBlockingRunnable(executor,
								() -> {
									List<String> lines = Files.readAllLines(expectedKeys);
									Map<String, String> collected = lines.stream()
											.map(s -> s.split(":"))
											.collect(toMap(parts -> parts[0], parts -> parts[1]));
									if (!collected.containsValue(key)) {
										int lastKey = collected.keySet().stream().mapToInt(Integer::valueOf).max().orElse(0);
										Files.write(expectedKeys, concat(lines, singletonList(++lastKey + ":" + key)));
									}
								})
								.then($ -> servlet.serve(request));
					}
				});
	}

	@Provides
	GlobalPmDriver<String> callsPMDriver(GlobalPmNode node) {
		return GlobalPmDriver.create(node, STRING_CODEC)
				.withIdGenerator(() -> ThreadLocalRandom.current().nextLong(JS_MIN_SAFE_INTEGER, JS_MAX_SAFE_INTEGER));
	}

	@Provides
	GlobalPmDriver<CreateSharedRepo> providePmDriver(GlobalPmNode node) {
		return GlobalPmDriver.create(node, SHARED_REPO_MESSAGE_CODEC);
	}

	@Provides
	@Named("Calls")
	AsyncServlet callsPMServlet(GlobalPmDriver<String> driver) {
		return GlobalPmDriverServlet.create(driver);
	}

	@Provides
	BiFunction<Eventloop, PrivKey, CommonUserContainer<ChatRoomOperation>> factory(OTDriver driver,
			GlobalPmDriver<CreateSharedRepo> pmDriver) {
		return (eventloop, privKey) -> {
			RepoID repoID = RepoID.of(privKey, CHAT_REPO_PREFIX);
			MyRepositoryId<ChatRoomOperation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, CHAT_ROOM_OPERATION_CODEC);
			return CommonUserContainer.create(eventloop, driver, CHAT_ROOM_OT_SYSTEM, myRepositoryId, pmDriver, CHAT_INDEX_REPO);
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
