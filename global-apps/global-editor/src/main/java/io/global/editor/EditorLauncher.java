package io.global.editor;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.editor.document.DocumentMultiOperation;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.SharedRepoModule;
import io.global.ot.contactlist.ContactsModule;
import io.global.ot.service.UserContainerModule;
import io.global.ot.shared.IndexRepoModule;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.override;

public final class EditorLauncher extends Launcher {
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
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create().printEffectiveConfig(),
				new EditorModule(),
				new ContactsModule(),
				new IndexRepoModule(EDITOR_INDEX_REPO),
				new SharedRepoModule<DocumentMultiOperation>(DOCUMENT_REPO_PREFIX) {},
				new UserContainerModule<DocumentMultiOperation>(EDITOR_INDEX_REPO, DOCUMENT_REPO_PREFIX) {},
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
		new EditorLauncher().launch(args);
	}
}
