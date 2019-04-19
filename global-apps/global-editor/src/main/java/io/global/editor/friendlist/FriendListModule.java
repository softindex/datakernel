package io.global.editor.friendlist;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.global.editor.DynamicOTNodeServlet;
import io.global.ot.client.OTDriver;

import static io.global.editor.friendlist.FriendListOTSystem.createOTSystem;
import static io.global.editor.friendlist.FriendListOperation.FRIEND_LIST_CODEC;

public final class FriendListModule extends AbstractModule {
	public static final String REPOSITORY_NAME = "friends";

	@Provides
	@Singleton
	DynamicOTNodeServlet<FriendListOperation> provideServlet(OTDriver driver) {
		return DynamicOTNodeServlet.create(driver, createOTSystem(), FRIEND_LIST_CODEC, REPOSITORY_NAME);
	}

}
