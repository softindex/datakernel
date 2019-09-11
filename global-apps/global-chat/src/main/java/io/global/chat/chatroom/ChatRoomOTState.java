package io.global.chat.chatroom;

import io.datakernel.ot.OTState;
import io.global.chat.chatroom.message.Message;
import io.global.chat.chatroom.operation.ChatRoomOperation;
import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public final class ChatRoomOTState implements OTState<ChatRoomOperation> {
	private final Set<Message> messages = new TreeSet<>(Comparator.comparingLong(Message::getTimestamp));
	private final Map<PubKey, Boolean> handled = new HashMap<>();

	@Nullable
	private CallInfo callInfo;

	@Override
	public void init() {
		messages.clear();
		handled.clear();
		callInfo = null;
	}

	@Override
	public void apply(ChatRoomOperation op) {
		op.apply(this);
	}

	public void setCallInfo(@Nullable CallInfo callInfo) {
		this.callInfo = callInfo;
	}

	@Nullable
	public CallInfo getCallInfo() {
		return callInfo;
	}

	public Set<Message> getMessages() {
		return messages;
	}

	public void addMessage(Message message) {
		messages.add(message);
	}

	public void removeMessage(Message message) {
		messages.remove(message);
	}

	public Map<PubKey, Boolean> getHandled() {
		return handled;
	}

	public boolean isEmpty() {
		return messages.isEmpty();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ChatRoomOTState state = (ChatRoomOTState) o;

		if (!messages.equals(state.messages)) return false;
		if (!handled.equals(state.handled)) return false;
		if (callInfo != null ? !callInfo.equals(state.callInfo) : state.callInfo != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = messages.hashCode();
		result = 31 * result + handled.hashCode();
		result = 31 * result + (callInfo != null ? callInfo.hashCode() : 0);
		return result;
	}
}
