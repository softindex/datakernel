package io.global.documents;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.common.BinaryDataFormats;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.MapModule;
import io.global.ot.OTAppCommonModule;
import io.global.ot.SharedRepoModule;
import io.global.ot.contactlist.ContactsModule;
import io.global.ot.contactlist.ContactsOperation;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.UserContainerModule;
import io.global.ot.shared.IndexRepoModule;
import io.global.ot.shared.SharedReposOperation;

import java.util.Comparator;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.edit.EditOTSystem.createOTSystem;

public final class GlobalDocumentsApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-documents.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Documents";
	private static final String DOCUMENT_REPO_PREFIX = "documents/document";
	private static final String DOCUMENTS_INDEX_REPO = "documents/index";
	private static final String PROFILE_REPO = "profile";

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
		return BinaryDataFormats.createGlobal()
				.with(EditOperation.class, EDIT_OPERATION_CODEC);
	}

	@Provides
	RoutingServlet provideMainServlet(
			DynamicOTNodeServlet<ContactsOperation> contactsServlet,
			DynamicOTNodeServlet<SharedReposOperation> documentListServlet,
			DynamicOTNodeServlet<EditOperation> documentServlet,
			DynamicOTNodeServlet<MapOperation<String, String>> profileServlet,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/contacts/*", contactsServlet)
				.map("/ot/documents/*", documentListServlet)
				.map("/ot/document/:suffix/*", documentServlet)
				.map("/ot/profile/:pubKey/*", profileServlet)
				.map("/ot/myProfile/*", profileServlet)
				.map("/*", staticServlet);
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
						bind(new Key<OTSystem<EditOperation>>() {}).toInstance(createOTSystem());
						bind(new Key<Comparator<String>>() {}).toInstance(String::compareTo);
						super.configure();
					}
				},
				new MapModule<String, String>(PROFILE_REPO) {},
				new ContactsModule(),
				new IndexRepoModule(DOCUMENTS_INDEX_REPO),
				new UserContainerModule<EditOperation>(DOCUMENTS_INDEX_REPO, DOCUMENT_REPO_PREFIX) {},
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
