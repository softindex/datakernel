package io.global.ot.chat;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.chat.chatroom.messages.Message;
import io.global.chat.chatroom.messages.MessageOperation;
import io.global.chat.chatroom.messages.MessagesOTSystem;
import io.global.common.ExampleCommonModule;
import io.global.common.ot.OTCommonModule;
import io.global.launchers.GlobalNodesModule;

import java.util.function.Function;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.global.chat.Utils.MESSAGE_OPERATION_CODEC;

public final class GlobalChatDemoApp extends Launcher {
	public static final String PROPERTIES_FILE = "client.properties";
	public static final String CREDENTIALS_FILE = "credentials.properties";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	public static final String DEFAULT_SERVER_ID = "Chat Node";
	public static final String DEFAULT_STATIC_PATH = "/build";
	public static final int CONTENT_MAX_LENGTH = 10;
	public static final Function<MessageOperation, String> DIFF_TO_STRING = op -> {
		Message message = op.getMessage();
		String author = message.getAuthor();
		String allContent = message.getContent();
		String content = allContent.length() > CONTENT_MAX_LENGTH ?
				(allContent.substring(0, CONTENT_MAX_LENGTH) + "...") :
				allContent;
		return (op.isTombstone() ? "-" : "+") + '[' + author + ':' + content + ']';
	};

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
				new OTCommonModule<MessageOperation>() {{
					bind(new Key<StructuredCodec<MessageOperation>>() {}).toInstance(MESSAGE_OPERATION_CODEC);
					bind(new Key<Function<MessageOperation, String>>() {}).toInstance(DIFF_TO_STRING);
					bind(new Key<OTSystem<MessageOperation>>() {}).toInstance(MessagesOTSystem.createOTSystem());
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
