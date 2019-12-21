package io.global.notes;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.debug.DebugViewerModule;
import io.global.kv.GlobalKvDriver;
import io.global.kv.api.KvClient;
import io.global.launchers.GlobalNodesModule;
import io.global.launchers.sync.KvSyncModule;
import io.global.ot.OTAppCommonModule;
import io.global.ot.OTGeneratorsModule;
import io.global.ot.TypedRepoNames;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerModule;
import io.global.ot.session.AuthModule;
import io.global.ot.session.UserId;
import io.global.session.KvSessionModule;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.global.Utils.DEFAULT_SYNC_SCHEDULE_CONFIG;
import static io.global.debug.DebugViewerModule.DebugView.KV;
import static io.global.debug.DebugViewerModule.DebugView.OT;

public final class GlobalNotesApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-notes.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Notes";
	private static final String SESSION_ID = "NOTES_SID";
	private static final Path DEFAULT_CONTAINERS_DIR = Paths.get("containers");

	@Inject
	AsyncHttpServer server;

	@Inject
	GlobalKvDriver<String, UserId> kvDriver;

	@Provides
	Config config() {
		return Config.create()
				.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
				.with("node.serverId", DEFAULT_SERVER_ID)
				.with("kv.catchUp.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.with("kv.push.schedule", DEFAULT_SYNC_SCHEDULE_CONFIG)
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	TypedRepoNames typedRepoNames() {
		return TypedRepoNames.create("global-notes")
				.withRepoName(new Key<MapOperation<String, String>>() {}, "notes")
				.withRepoPrefix(new Key<EditOperation>() {}, "note")
				.withRepoName(new Key<KvClient<String, UserId>>() {}, "session");
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new GlobalNotesModule(),
				new OTAppCommonModule(),
				new AuthModule(SESSION_ID),
				OTGeneratorsModule.create(),
				new KvSessionModule(),
				new DebugViewerModule(OT, KV),
				new ContainerModule<GlobalNotesContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				override(new GlobalNodesModule(),
						new LocalNodeCommonModule(DEFAULT_SERVER_ID)),
				KvSyncModule.create()
						.withCatchUp()
						.withPush()
		);
	}

	@Override
	public void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalNotesApp().launch(args);
	}
}
