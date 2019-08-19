package io.global.chat;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.chat.chatroom.Message;
import io.global.common.BinaryDataFormats;
import io.global.common.PrivKey;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.MapModule;
import io.global.ot.SharedRepoModule;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsModule;
import io.global.ot.service.CommonUserContainer;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.set.SetOperation;
import io.global.ot.shared.IndexRepoModule;
import io.global.pm.GlobalPmDriver;

import java.util.Comparator;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.override;
import static io.global.chat.Utils.MESSAGE_OPERATION_CODEC;
import static io.global.chat.Utils.MESSAGE_OT_SYSTEM;

public final class GlobalChatApp extends Launcher {
	private static final String PROPERTIES_FILE = "chat.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Chat";
	private static final String CHAT_REPO_PREFIX = "chat/room";
	private static final String CHAT_INDEX_REPO = "chat/index";
	private static final String PROFILE_REPO_NAME = "profile";

	@Inject
	@Named("Chat")
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
	CodecFactory registry() {
		return BinaryDataFormats.createGlobal();
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new ChatModule(),
				new ContactsModule(),
				new MapModule<String, String>(PROFILE_REPO_NAME) {},
				new IndexRepoModule(CHAT_INDEX_REPO),
				new ContainerModule<CommonUserContainer<SetOperation<Message>>>() {
					@Provides
					BiFunction<Eventloop, PrivKey, CommonUserContainer<SetOperation<Message>>> factory(OTDriver driver,
							GlobalPmDriver<CreateSharedRepo> pmDriver) {
						return (eventloop, privKey) -> {
							RepoID repoID = RepoID.of(privKey, CHAT_REPO_PREFIX);
							MyRepositoryId<SetOperation<Message>> myRepositoryId = new MyRepositoryId<>(repoID, privKey, MESSAGE_OPERATION_CODEC);
							return CommonUserContainer.create(eventloop, driver, MESSAGE_OT_SYSTEM, myRepositoryId, pmDriver, CHAT_INDEX_REPO);
						};
					}
				},
				new SharedRepoModule<SetOperation<Message>>(CHAT_REPO_PREFIX) {},
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
