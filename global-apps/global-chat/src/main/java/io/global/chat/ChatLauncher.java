package io.global.chat;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.async.Promise;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.chat.chatroom.RoomModule;
import io.global.chat.friendlist.FriendListModule;
import io.global.chat.roomlist.RoomListModule;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.api.SharedKeyStorage;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.common.stub.InMemorySharedKeyStorage;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.server.CommitStorage;
import io.global.ot.stub.CommitStorageStub;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;

public final class ChatLauncher extends Launcher {
	private static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	private static final String PROPERTIES_FILE = "chat.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "http://127.0.0.1:9000";

	@Inject
	@Named("Chat")
	AsyncHttpServer server;

	@Override
	public Collection<com.google.inject.Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.with("node.serverId", DEFAULT_SERVER_ID)
								.override(Config.ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new RoomModule(),
				new ChatModule(),
				new FriendListModule(),
				new RoomListModule(),
				// override for debug purposes
				override(new GlobalNodesModule())
						.with(new AbstractModule() {
							@Provides
							@Singleton
							DiscoveryService provideDiscoveryService(Eventloop eventloop) {
								AnnouncementStorage announcementStorage = getDebugAnnouncementStorage();
								SharedKeyStorage sharedKeyStorage = new InMemorySharedKeyStorage();
								return LocalDiscoveryService.create(eventloop, announcementStorage, sharedKeyStorage);
							}

							@Provides
							@Singleton
							CommitStorage provideCommitStorage() {
								return new CommitStorageStub();
							}
						})
		);
	}

	@NotNull
	private AnnouncementStorage getDebugAnnouncementStorage() {
		// Makes a local node - a single master for all public keys
		return new AnnouncementStorage() {
			@Override
			public Promise<Void> store(PubKey space, SignedData<AnnounceData> announceData) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Promise<@Nullable SignedData<AnnounceData>> load(PubKey ignored) {
				return Promise.of(SignedData.sign(
						REGISTRY.get(AnnounceData.class),
						AnnounceData.of(1, singleton(new RawServerId(DEFAULT_SERVER_ID))),
						KeyPair.generate().getPrivKey())
				);
			}
		};
	}

	@Override
	public void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new ChatLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
