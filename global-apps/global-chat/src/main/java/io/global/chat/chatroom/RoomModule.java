package io.global.chat.chatroom;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.global.chat.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.chat.Utils.CHAT_ROOM_CODEC;
import static io.global.chat.Utils.createMergedOTSystem;

public final class RoomModule extends AbstractModule {
	public static final String ROOM_PREFIX = "chat/room";

	@Provides
	@Singleton
	DynamicOTNodeServlet<ChatMultiOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createMergedOTSystem(), CHAT_ROOM_CODEC, ROOM_PREFIX);
	}
}
