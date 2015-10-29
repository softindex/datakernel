package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

class RequestSenderTypeDispatcher implements RequestSender {
	private static final RpcSenderNotSpecifiedException SENDER_IS_NOT_SPECIFIED_EXCEPTION
			= new RpcSenderNotSpecifiedException("Sender is not specified for this type of request");
	private static final RpcNoSenderAvailableException NO_ACTIVE_SENDER_AVAILABLE_EXCEPTION
			= new RpcNoSenderAvailableException("No active senders available");

	private Map<Class<? extends RpcMessageData>, RequestSender> dataTypeToSender;
	private RequestSender defaultSender;
	private final boolean active;

	public RequestSenderTypeDispatcher(Map<Class<? extends RpcMessageData>, RequestSender> dataTypeToSender,
	                                   RequestSender defaultSender) {
		this.dataTypeToSender = dataTypeToSender;
		this.defaultSender = defaultSender;
		this.active = countActiveSenders(dataTypeToSender) > 0 || (defaultSender != null && defaultSender.isActive());
	}

	@Override
	public <T extends RpcMessageData> void sendRequest(RpcMessageData request,int timeout, ResultCallback<T> callback) {
		checkNotNull(callback);

		assert isActive();

		RequestSender specifiedSender = dataTypeToSender.get(request);
		RequestSender sender = specifiedSender != null ? specifiedSender : defaultSender;
		if (sender != null) {
			if (sender.isActive()) {
				sender.sendRequest(request, timeout, callback);
			} else {
				callback.onException(NO_ACTIVE_SENDER_AVAILABLE_EXCEPTION);
			}
		} else {
			callback.onException(SENDER_IS_NOT_SPECIFIED_EXCEPTION);
		}
	}

	@Override
	public boolean isActive() {
		return active;
	}

	private static int countActiveSenders(Map<Class<? extends RpcMessageData>, RequestSender> keyToSender) {
		int count = 0;
		for (RequestSender sender : keyToSender.values()) {
			if (sender.isActive()) {
				++count;
			}
		}
		return count;
	}
}
