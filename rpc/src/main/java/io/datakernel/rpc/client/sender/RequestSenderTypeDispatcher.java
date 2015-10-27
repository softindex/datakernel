package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;

import java.util.Map;

import static io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

public class RequestSenderTypeDispatcher implements RequestSender {
	private static final RpcNoSenderAvailableException SENDER_IS_NOT_SPECIFIED_EXCEPTION
			= new RpcNoSenderAvailableException("Sender is not specified for this type of request");
	private static final RpcNoSenderAvailableException NO_ACTIVE_SENDER_AVAILABLE_EXCEPTION
			= new RpcNoSenderAvailableException("No active senders available");

	private Map<Class<? extends RpcMessageData>, RequestSender> dataTypeToSender;
	private RequestSender defaultSender;

	public RequestSenderTypeDispatcher(Map<Class<? extends RpcMessageData>, RequestSender> dataTypeToSender,
	                                   RequestSender defaultSender) {
		this.dataTypeToSender = dataTypeToSender;
		this.defaultSender = defaultSender;
	}

	@Override
	public <T extends RpcMessageData> void sendRequest(RpcMessageData request,int timeout, ResultCallback<T> callback) {
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
		return false;
	}
}
