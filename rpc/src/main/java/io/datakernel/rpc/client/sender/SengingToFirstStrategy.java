package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

final class SengingToFirstStrategy extends RequestSendingStrategyToGroup {

	public SengingToFirstStrategy(List<RequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public SengingToFirstStrategy(List<RequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RequestSender createSenderInstance(List<RequestSender> subSenders) {
		return new RequestSenderToFirst(subSenders);
	}

	final static class RequestSenderToFirst implements RequestSender{
		private List<RequestSender> subSenders;

		public RequestSenderToFirst(List<RequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0);
			this.subSenders = senders;
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender first = subSenders.get(0);
			first.sendRequest(request, timeout, callback);
		}
	}
}
