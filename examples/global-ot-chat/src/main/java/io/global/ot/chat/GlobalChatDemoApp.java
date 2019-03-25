package io.global.ot.chat;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.chat.chatroom.messages.MessageOperation;
import io.global.common.ExampleCommonModule;
import io.global.common.ot.OTCommonModule;
import io.global.launchers.GlobalNodesModule;

import java.util.Collection;
import java.util.function.Function;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.global.chat.Utils.MESSAGE_OPERATION_CODEC;
import static io.global.chat.chatroom.messages.MessagesOTSystem.createOTSystem;
import static io.global.ot.chat.operations.Utils.DIFF_TO_STRING;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public final class GlobalChatDemoApp extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "client.properties";
	public static final String CREDENTIALS_FILE = "credentials.properties";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	public static final String DEFAULT_SERVER_ID = "Chat Node";

	@Inject
	@Named("Example")
	AsyncHttpServer chatServer;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.with("node.serverId", DEFAULT_SERVER_ID)
								.override(Config.ofProperties(PROPERTIES_FILE, true)
										.combine(Config.ofProperties(CREDENTIALS_FILE, true)))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new OTCommonModule<MessageOperation>() {
					@Override
					protected void configure() {
						bind(new TypeLiteral<StructuredCodec<MessageOperation>>() {}).toInstance(MESSAGE_OPERATION_CODEC);
						bind(new TypeLiteral<Function<MessageOperation, String>>() {}).toInstance(DIFF_TO_STRING);
						bind(new TypeLiteral<OTSystem<MessageOperation>>() {}).toInstance(createOTSystem());
					}
				},
				override(new GlobalNodesModule())
						.with(new ExampleCommonModule())
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalChatDemoApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
