package io.global.chat.chatroom.operation;

import io.global.chat.chatroom.CallInfo;
import io.global.chat.chatroom.ChatRoomOTState;
import io.global.chat.chatroom.message.Message;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

import static io.global.chat.Utils.toCallMessage;

public final class CallOperation implements ChatRoomOperation {
	@Nullable
	private final CallInfo prev;
	@Nullable
	private final CallInfo next;

	public CallOperation(@Nullable CallInfo prev, @Nullable CallInfo next) {
		this.prev = prev;
		this.next = next;
	}

	public static CallOperation call(CallInfo next) {
		return new CallOperation(null, next);
	}

	public static CallOperation call(CallInfo prev, CallInfo next) {
		return new CallOperation(prev, next);
	}

	@Override
	public void apply(ChatRoomOTState state) {
		if (prev == null) {
			assert next != null;
			Message callMessage = toCallMessage(next);
			assert !state.getMessages().contains(callMessage);
			state.addMessage(callMessage);
		} else if (next == null) {
			Message callMessage = toCallMessage(prev);
			assert state.getMessages().contains(callMessage);
			state.removeMessage(callMessage);
		}
		assert Objects.equals(prev, state.getCallInfo());
		state.setCallInfo(next);
	}

	@Override
	public boolean isEmpty() {
		return Objects.equals(prev, next);
	}

	@Override
	public CallOperation invert() {
		return new CallOperation(next, prev);
	}

	@Nullable
	public CallInfo getPrev() {
		return prev;
	}

	@Nullable
	public CallInfo getNext() {
		return next;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CallOperation that = (CallOperation) o;
		return Objects.equals(prev, that.prev) &&
				Objects.equals(next, that.next);
	}

	@Override
	public int hashCode() {
		return Objects.hash(prev, next);
	}

	@Override
	public String toString() {
		return "CallOperation{" +
				"prev=" + prev +
				", next=" + next +
				'}';
	}
}
