package io.global.chat;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.MiddlewareServlet;
import io.global.chat.chatroom.ChatMultiOperation;
import io.global.chat.friendlist.FriendListOperation;
import io.global.chat.roomlist.RoomListOperation;
import io.global.common.SimKey;
import io.global.ot.client.OTDriver;
import io.global.ot.server.GlobalOTNodeImpl;

import static io.datakernel.launchers.initializers.Initializers.ofHttpServer;
import static io.global.launchers.GlobalConfigConverters.ofSimKey;

public final class ChatModule extends AbstractModule {
	private static final SimKey DEMO_SIM_KEY = SimKey.of(new byte[]{2, 51, -116, -111, 107, 2, -50, -11, -16, -66, -38, 127, 63, -109, -90, -51});

	@Provides
	@Singleton
	@Named("Chat")
	AsyncHttpServer provideServer(Eventloop eventloop, MiddlewareServlet servlet, Config config) {
		return AsyncHttpServer.create(eventloop, servlet)
				.initialize(ofHttpServer(config.getChild("http")));
	}

	@Provides
	@Singleton
	MiddlewareServlet provideMainServlet(
			DynamicOTNodeServlet<FriendListOperation> friendsListServlet,
			DynamicOTNodeServlet<RoomListOperation> roomListServlet,
			DynamicOTNodeServlet<ChatMultiOperation> roomServlet
	) {
		return MiddlewareServlet.create()
				.with("/friendList", friendsListServlet)
				.with("/roomList", roomListServlet)
				.with("/room/:suffix", roomServlet);
	}

	@Provides
	@Singleton
	OTDriver provideDriver(Eventloop eventloop, GlobalOTNodeImpl node, Config config) {
		SimKey simKey = config.get(ofSimKey(), "credentials.simKey", DEMO_SIM_KEY);
		return new OTDriver(node, simKey);
	}
}
