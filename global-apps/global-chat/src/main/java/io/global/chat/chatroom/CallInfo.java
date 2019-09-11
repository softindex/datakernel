package io.global.chat.chatroom;

import io.global.common.PubKey;
import org.jetbrains.annotations.Nullable;

public final class CallInfo {
	private final PubKey author;
	private final long callStarted;

	public CallInfo(PubKey author, long callStarted) {
		this.author = author;
		this.callStarted = callStarted;
	}

	public PubKey getAuthor() {
		return author;
	}

	public long getCallStarted() {
		return callStarted;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CallInfo callInfo = (CallInfo) o;

		if (callStarted != callInfo.callStarted) return false;
		if (!author.equals(callInfo.author)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = author.hashCode();
		result = 31 * result + (int) (callStarted ^ (callStarted >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "CallInfo{" +
				"author=" + author +
				", callStarted=" + callStarted +
				'}';
	}
}
