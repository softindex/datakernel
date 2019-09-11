package io.global.chat.chatroom.operation;

import io.global.chat.chatroom.CallInfo;
import io.global.chat.chatroom.ChatRoomOTState;
import io.global.chat.chatroom.message.Message;
import io.global.common.PubKey;

import java.util.Map;
import java.util.Objects;

import static io.global.chat.Utils.EMPTY_CALL_INFO;
import static io.global.chat.Utils.toDropMessage;
import static java.util.Collections.emptyMap;

public final class DropCallOperation implements ChatRoomOperation {
	public static final DropCallOperation EMPTY = new DropCallOperation(EMPTY_CALL_INFO, emptyMap(), -1, false);

	private final CallInfo callInfo;
	private final Map<PubKey, Boolean> previouslyHandled;
	private final long dropTimestamp;
	private final boolean invert;

	public DropCallOperation(CallInfo callInfo, Map<PubKey, Boolean> previouslyHandled, long dropTimestamp, boolean invert) {
		this.callInfo = callInfo;
		this.previouslyHandled = previouslyHandled;
		this.dropTimestamp = dropTimestamp;
		this.invert = invert;
	}

	public static DropCallOperation dropCall(CallInfo callInfo, Map<PubKey, Boolean> previouslyHandled, long dropTimestamp) {
		return new DropCallOperation(callInfo, previouslyHandled, dropTimestamp, false);
	}

	@Override
	public void apply(ChatRoomOTState state) {
		Message systemMessage = toDropMessage(this);
		Map<PubKey, Boolean> handled = state.getHandled();
		if (invert) {
			assert state.getCallInfo() == null;
			state.setCallInfo(callInfo);
			assert state.getMessages().contains(systemMessage);
			state.removeMessage(systemMessage);
			assert handled.isEmpty();
			handled.putAll(previouslyHandled);
		} else {
			assert Objects.equals(state.getCallInfo(), callInfo);
			state.setCallInfo(null);
			assert !state.getMessages().contains(systemMessage);
			state.addMessage(systemMessage);
			assert handled.equals(previouslyHandled);
			handled.clear();
		}
	}

	@Override
	public boolean isEmpty() {
		return dropTimestamp == -1;
	}

	@Override
	public DropCallOperation invert() {
		return new DropCallOperation(callInfo, previouslyHandled, dropTimestamp, !invert);
	}

	public CallInfo getCallInfo() {
		return callInfo;
	}

	public Map<PubKey, Boolean> getPreviouslyHandled() {
		return previouslyHandled;
	}

	public long getDropTimestamp() {
		return dropTimestamp;
	}

	public boolean isInvert() {
		return invert;
	}

	public boolean isInversionFor(DropCallOperation other) {
		return callInfo.equals(other.callInfo) &&
				previouslyHandled.equals(other.previouslyHandled) &&
				dropTimestamp == other.dropTimestamp &&
				invert != other.invert;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) return true;
		if (other == null || getClass() != other.getClass()) return false;

		DropCallOperation o = (DropCallOperation) other;

		if (dropTimestamp != o.dropTimestamp) return false;
		if (invert != o.invert) return false;
		if (!callInfo.equals(o.callInfo)) return false;
		if (!previouslyHandled.equals(o.previouslyHandled)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = callInfo.hashCode();
		result = 31 * result + previouslyHandled.hashCode();
		result = 31 * result + (int) (dropTimestamp ^ (dropTimestamp >>> 32));
		result = 31 * result + (invert ? 1 : 0);
		return result;
	}

	@Override
	public String toString() {
		return "DropCallOperation{" +
				"callInfo=" + callInfo +
				", previouslyHandled=" + previouslyHandled +
				", dropTimestamp=" + dropTimestamp +
				", invert=" + invert +
				'}';
	}
}
