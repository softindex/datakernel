package io.global.editor;

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
import io.global.common.PrivKey;
import io.global.editor.document.DocumentMultiOperation;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.SharedRepoModule;
import io.global.ot.api.RepoID;
import io.global.ot.client.MyRepositoryId;
import io.global.ot.client.OTDriver;
import io.global.ot.contactlist.ContactsModule;
import io.global.ot.service.CommonUserContainer;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.messaging.CreateSharedRepo;
import io.global.ot.shared.IndexRepoModule;
import io.global.pm.GlobalPmDriver;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.override;
import static io.global.editor.Utils.DOCUMENT_MULTI_OPERATION_CODEC;
import static io.global.editor.Utils.createMergedOTSystem;

public final class GlobalEditorApp extends Launcher {
	private static final String PROPERTIES_FILE = "editor.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Editor";
	private static final String DOCUMENT_REPO_PREFIX = "editor/document";
	private static final String EDITOR_INDEX_REPO = "editor/index";

	@Inject
	@Named("Editor")
	AsyncHttpServer server;

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
				.with("node.serverId", DEFAULT_SERVER_ID)
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new EditorModule(),
				new ContactsModule(),
				new IndexRepoModule(EDITOR_INDEX_REPO),
				new SharedRepoModule<DocumentMultiOperation>(DOCUMENT_REPO_PREFIX) {},
				new ContainerModule<CommonUserContainer<DocumentMultiOperation>>() {
					@Provides
					BiFunction<Eventloop, PrivKey, CommonUserContainer<DocumentMultiOperation>> factory(OTDriver driver,
							GlobalPmDriver<CreateSharedRepo> pmDriver) {
						return (eventloop, privKey) -> {
							RepoID repoID = RepoID.of(privKey, DOCUMENT_REPO_PREFIX);
							MyRepositoryId<DocumentMultiOperation> myRepositoryId = new MyRepositoryId<>(repoID, privKey, DOCUMENT_MULTI_OPERATION_CODEC);
							return CommonUserContainer.create(eventloop, driver, createMergedOTSystem(), myRepositoryId, pmDriver, EDITOR_INDEX_REPO);
						};
					}
				},
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
		new GlobalEditorApp().launch(args);
	}
}
