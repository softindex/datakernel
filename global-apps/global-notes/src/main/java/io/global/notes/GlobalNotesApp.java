package io.global.notes;

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
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.common.PrivKey;
import io.global.kv.GlobalKvDriver;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.*;
import io.global.ot.client.OTDriver;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOperation;
import io.global.ot.service.ContainerModule;
import io.global.ot.service.ContainerScope;
import io.global.ot.service.SimpleUserContainer;
import io.global.ot.session.AuthModule;
import io.global.ot.session.KvSessionStore;
import io.global.ot.session.UserId;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.edit.EditOTSystem.createOTSystem;

public final class GlobalNotesApp extends Launcher {
	private static final String PROPERTIES_FILE = "global-notes.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Notes";
	private static final String NOTES_INDEX_REPO = "notes/index";
	private static final String NOTES_SESSION_TABLE = "notes/session";
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
				.overrideWith(Config.ofProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Provides
	AsyncServlet containerServlet(
			DynamicOTUplinkServlet<MapOperation<String, String>> notesServlet,
			DynamicOTUplinkServlet<EditOperation> noteServlet,
			DynamicOTGraphServlet<EditOperation> graphServlet,
			@Named("authorization") RoutingServlet authorizationServlet,
			@Named("session") AsyncServletDecorator sessionDecorator,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/*", sessionDecorator.serve(RoutingServlet.create()
						.map("/notes/*", notesServlet)
						.map("/note/:suffix/*", noteServlet)
						.map("/graph/:suffix", graphServlet)))
				.map("/*", staticServlet)
				.merge(authorizationServlet);
	}

	@Provides
	CodecFactory codecFactory() {
		return OTUtils.createOTRegistry()
				.with(EditOperation.class, EDIT_OPERATION_CODEC);
	}

	@Provides
	DynamicOTUplinkServlet<EditOperation> noteServlet(OTDriver driver) {
		return DynamicOTUplinkServlet.create(driver, createOTSystem(), EDIT_OPERATION_CODEC, NOTES_INDEX_REPO);
	}

	@Provides
	DynamicOTGraphServlet<EditOperation> noteGraphServlet(DynamicOTUplinkServlet<EditOperation> noteServlet) {
		return noteServlet.createGraphServlet(Objects::toString);
	}

	@Provides
	@ContainerScope
	SimpleUserContainer userContainer(Eventloop eventloop, PrivKey privKey, GlobalKvDriver<String, UserId> kvDriver) {
		KvSessionStore<UserId> sessionStore = KvSessionStore.create(eventloop, kvDriver.adapt(privKey), NOTES_SESSION_TABLE);
		return SimpleUserContainer.create(eventloop, privKey.computeKeys(), sessionStore);
	}

	@Override
	public Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				new OTAppCommonModule(),
				new AuthModule<SimpleUserContainer>(SESSION_ID) {},
				new ContainerModule<SimpleUserContainer>() {}
						.rebindImport(Path.class, Binding.to(config -> config.get(ofPath(), "containers.dir", DEFAULT_CONTAINERS_DIR), Config.class)),
				new MapModule<String, String>(NOTES_INDEX_REPO) {},
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
		new GlobalNotesApp().launch(args);
	}
}
