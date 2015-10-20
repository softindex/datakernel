package io.datakernel.rpc.client.sender;

import static io.datakernel.codegen.utils.Preconditions.checkNotNull;

import java.util.List;

abstract class RequestSenderToGroup implements RequestSender {

	private final List<RequestSender> subSenders;
	private Integer key = null;

	public RequestSenderToGroup(List<RequestSender> senders) {
		this.subSenders = checkNotNull(senders);
	}

	protected List<RequestSender> getSubSenders() {
		return subSenders;
	}

	protected abstract int getHashBase();

	@Override
	public int getKey() {
		if (key == null) {
			int hash = getHashBase();
			for (RequestSender subSender : subSenders) {
				hash = 31*hash + subSender.getKey();
			}
			key = hash;
		}
		// TODO (vmykhalko): is it correct to assume that key is immutable?
		return key;
	}

	@Override
	public boolean isActive() {
		// TODO (vmykhalko): do recursive calls of isActive() affect performance noticeably?
		for (RequestSender subSender : subSenders) {
			if (subSender.isActive()) {
				return true;
			}
		}
		return false;
	}
}
