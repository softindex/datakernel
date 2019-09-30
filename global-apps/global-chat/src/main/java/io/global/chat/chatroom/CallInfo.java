package io.global.chat.chatroom;

import io.global.common.PubKey;

public final class CallInfo {
	private final PubKey pubKey;
	private final String peerId;
	private final long callStarted;

	public CallInfo(PubKey pubKey, String peerId, long callStarted) {
		this.pubKey = pubKey;
		this.peerId = peerId;
		this.callStarted = callStarted;
	}

	public PubKey getPubKey() {
		return pubKey;
	}

	public String getPeerId() {
		return peerId;
	}

	public long getCallStarted() {
		return callStarted;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CallInfo callInfo = (CallInfo) o;

		if (callStarted != callInfo.callStarted) return false;
		if (!pubKey.equals(callInfo.pubKey)) return false;
		if (!peerId.equals(callInfo.peerId)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = pubKey.hashCode();
		result = 31 * result + peerId.hashCode();
		result = 31 * result + (int) (callStarted ^ (callStarted >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "CallInfo{" +
				"pubKey=" + pubKey +
				", peerId='" + peerId + '\'' +
				", callStarted=" + callStarted +
				'}';
	}
}
