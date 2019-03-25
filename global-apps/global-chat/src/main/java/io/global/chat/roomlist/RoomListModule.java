package io.global.chat.roomlist;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.global.chat.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.chat.Utils.ROOM_LIST_OPERATION__CODEC;
import static io.global.chat.roomlist.RoomListOTSystem.createOTSystem;

public final class RoomListModule extends AbstractModule {
	public static final String REPOSITORY_NAME = "roomList";

	@Provides
	@Singleton
	DynamicOTNodeServlet<RoomListOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), ROOM_LIST_OPERATION__CODEC, REPOSITORY_NAME);
	}
}
