package io.global.notes;

import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.RoutingServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;
import io.global.LocalNodeCommonModule;
import io.global.common.BinaryDataFormats;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.DynamicOTGraphServlet;
import io.global.ot.DynamicOTNodeServlet;
import io.global.ot.MapModule;
import io.global.ot.OTAppCommonModule;
import io.global.ot.client.OTDriver;
import io.global.ot.edit.EditOperation;
import io.global.ot.map.MapOperation;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.OTUtils.EDIT_OPERATION_CODEC;
import static io.global.ot.edit.EditOTSystem.createOTSystem;

public final class GlobalNotesLauncher extends Launcher {
	private static final String PROPERTIES_FILE = "notes.properties";
	private static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	private static final String DEFAULT_SERVER_ID = "Global Notes";
	private static final String NOTES_INDEX_REPO = "notes/index";

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
	AsyncServlet mainServlet(
			DynamicOTNodeServlet<MapOperation<String, String>> notesServlet,
			DynamicOTNodeServlet<EditOperation> noteServlet,
			DynamicOTGraphServlet<EditOperation> graphServlet,
			StaticServlet staticServlet
	) {
		return RoutingServlet.create()
				.map("/ot/notes/*", notesServlet)
				.map("/ot/note/:suffix/*", noteServlet)
				.map("/ot/graph/:suffix", graphServlet)
				.map("/*", staticServlet);
	}

	@Provides
	CodecFactory codecFactory() {
		return BinaryDataFormats.createGlobal()
				.with(EditOperation.class, EDIT_OPERATION_CODEC);
	}

	@Provides
	DynamicOTNodeServlet<EditOperation> noteServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), EDIT_OPERATION_CODEC, NOTES_INDEX_REPO);
	}

	@Provides
	DynamicOTGraphServlet<EditOperation> noteGraphServlet(DynamicOTNodeServlet<EditOperation> noteServlet) {
		return noteServlet.createGraphServlet(Objects::toString);
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
						bind(new Key<Comparator<String>>() {}).toInstance(String::compareTo);
						super.configure();
					}
				},
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
		new GlobalNotesLauncher().launch(args);
	}
}
