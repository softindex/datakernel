package io.global.chat.chatroom.operation;

import io.global.chat.chatroom.ChatRoomOTState;

public interface ChatRoomOperation {
	void apply(ChatRoomOTState state);

	boolean isEmpty();

	ChatRoomOperation invert();
}
