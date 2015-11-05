package io.datakernel.rpc.client.sender.helper;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.sender.RequestSender;
import io.datakernel.rpc.protocol.RpcMessage;

public class RequestSenderStub implements RequestSender {

	private final int id;
	private int sendRequestCalls;

	public RequestSenderStub(int id) {
		this.id = id;
		this.sendRequestCalls = 0;
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
	                                                              ResultCallback<T> callback) {
		++sendRequestCalls;
	}

	public int getId() {
		return id;
	}

	public int getSendRequestCalls() {
		return sendRequestCalls;
	}

	@Override
	public boolean equals(Object obj) {
		return ((RequestSenderStub) obj).getId() == id;
	}

	@Override
	public int hashCode() {
		return id;
	}
}
