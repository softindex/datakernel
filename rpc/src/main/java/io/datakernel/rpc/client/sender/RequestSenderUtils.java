package io.datakernel.rpc.client.sender;

import java.util.List;

final class RequestSenderUtils {
	public static final int EMPTY_KEY = -1;

	private RequestSenderUtils() {};

	public static List<RequestSender> checkAllSendersHaveKey(List<RequestSender> senders) throws IllegalArgumentException {
		for (RequestSender sender : senders) {
			if (sender.getKey() == EMPTY_KEY) {
				throw new IllegalArgumentException("One or more senders don't have key");
			}
		}
		return senders;
	}
}
