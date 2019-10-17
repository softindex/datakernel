package io.global.documents;

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
import io.global.ot.edit.EditOTSystem;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.CommonUserContainer;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.session.AuthModule;
import io.global.ot.session.KvSessionStore;
import io.global.ot.session.UserId;
import io.global.ot.shared.IndexRepoModule;
import io.global.ot.shared.SharedReposOperation;
import io.global.pm.Messenger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import static io.datakernel.codec.StructuredCodecs.LONG_CODEC;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.OTUtils.SHARED_REPO_MESSAGE_CODEC;

public final class GlobalDocumentsApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-documents.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Documents";
	private static final String DOCUMENT_REPO_PREFIX = "documents/document";
	private static final String DOCUMENTS_INDEX_REPO = "documents/index";
	private static final String DOCUMENTS_SESSION_TABLE = "documents/session";
	private static final String PROFILE_REPO = "profile";
	private static final String SESSION_ID = "DOCUMENTS_SID";
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
	CodecFactory codecFactory() {
		return OTUtils.createOTRegistry()
				.with(EditOperation.class, EDIT_OPERATION_CODEC);
	}

	@Provides
	Messenger<Long, CreateSharedRepo> messenger(GlobalKvNode node) {
		Random random = new Random();
		return Messenger.create(node, LONG_CODEC, SHARED_REPO_MESSAGE_CODEC, random::nextLong);
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTNodeServlet<ContactsOperation> contactsServlet,
			DynamicOTNodeServlet<SharedReposOperation> documentListServlet,
			DynamicOTNodeServlet<EditOperation> documentServlet,
			DynamicOTNodeServlet<MapOperation<String, String>> profileServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/*", sessionDecorator.serve(RoutingServlet.create()
						.map("/contacts/*", contactsServlet)
						.map("/documents/*", documentListServlet)
						.map("/document/:suffix/*", documentServlet)
						.map("/profile/:pubKey/*", profileServlet)
						.map("/myProfile/*", profileServlet)))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
	}

	@Provides
	BiFunction<Eventloop, PrivKey, CommonUserContainer<EditOperation>> factory(OTDriver driver, GlobalKvDriver<String, UserId> kvDriver,
			Messenger<Long, CreateSharedRepo> messenger) {
		return (eventloop, privKey) -> {
			RepoID repoID = RepoID.of(privKey, DOCUMENT_REPO_PREFIX);
			MyRepositoryId<EditOperation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, EDIT_OPERATION_CODEC);
			KvSessionStore<UserId> sessionStore = KvSessionStore.create(eventloop, kvDriver.adapt(privKey), DOCUMENTS_SESSION_TABLE);
			return CommonUserContainer.create(eventloop, driver, EditOTSystem.createOTSystem(), myRepositoryId, messenger, sessionStore, DOCUMENTS_INDEX_REPO);
		};
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new OTAppCommonModule() {
					@Override
					protected void configure() {
						bind(new Key<OTSystem<EditOperation>>() {}).toInstance(EditOTSystem.createOTSystem());
						super.configure();
					}
				},
				new AuthModule<CommonUserContainer<EditOperation>>(SESSION_ID) {},
				new MapModule<String, String>(PROFILE_REPO) {},
				new ContactsModule(),
				new IndexRepoModule(DOCUMENTS_INDEX_REPO),
				new ContainerModule<CommonUserContainer<EditOperation>>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new SharedRepoModule<EditOperation>(DOCUMENT_REPO_PREFIX) {},
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
		new GlobalDocumentsApp().launch(args);
	}
}
