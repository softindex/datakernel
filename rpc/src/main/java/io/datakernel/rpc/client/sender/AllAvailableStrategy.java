package io.datakernel.rpc.client.sender;

import io.datakernel.async.FirstResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AllAvailableStrategy extends RequestSendingStrategyToGroup{

	public AllAvailableStrategy(List<RequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public AllAvailableStrategy(List<RequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RequestSender createSenderInstance(List<RequestSender> subSenders) {
		return new RequestSenderToAll(subSenders);
	}

	final static class RequestSenderToAll implements RequestSender {

		private List<RequestSender> subSenders;

		public RequestSenderToAll(List<RequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0);
			this.subSenders = senders;
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              final ResultCallback<T> callback) {
			checkNotNull(callback);

			FirstResultCallback<T> resultCallback = new FirstResultCallback<>(callback);
			for (RequestSender sender : subSenders) {
				sender.sendRequest(request, timeout, resultCallback);
			}
			resultCallback.resultOf(subSenders.size());
		}
	}
}
