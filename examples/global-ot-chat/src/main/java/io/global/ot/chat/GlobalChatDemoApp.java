package io.global.ot.chat;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.core.Key;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.ExampleCommonModule;
import io.global.common.ot.OTCommonModule;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.chat.operations.ChatOperation;
import io.global.ot.chat.operations.Utils;

import java.util.function.Function;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.chat.operations.ChatOperation.OPERATION_CODEC;
import static io.global.ot.chat.operations.Utils.DIFF_TO_STRING;

public final class GlobalChatDemoApp extends Launcher {
	public static final String PROPERTIES_FILE = "client.properties";
	public static final String CREDENTIALS_FILE = "credentials.properties";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	public static final String DEFAULT_SERVER_ID = "Chat Node";
	public static final String DEFAULT_STATIC_PATH = "/build";

	@Inject
	@Named("Example")
	AsyncHttpServer chatServer;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.with("node.serverId", DEFAULT_SERVER_ID)
								.with("resources.path", DEFAULT_STATIC_PATH)
								.override(Config.ofClassPathProperties(PROPERTIES_FILE, true)
										.combine(Config.ofClassPathProperties(CREDENTIALS_FILE, true)))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new OTCommonModule<ChatOperation>() {{
					bind(new Key<StructuredCodec<ChatOperation>>() {}).toInstance(OPERATION_CODEC);
					bind(new Key<Function<ChatOperation, String>>() {}).toInstance(DIFF_TO_STRING);
					bind(new Key<OTSystem<ChatOperation>>() {}).to(Utils::createOTSystem);
				}},
				override(new GlobalNodesModule(), new ExampleCommonModule())
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalChatDemoApp().launch(args);
	}
}
