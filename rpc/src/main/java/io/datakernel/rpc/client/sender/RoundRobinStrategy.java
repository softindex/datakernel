package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class RoundRobinStrategy extends RequestSendingStrategyToGroup {

	public RoundRobinStrategy(List<RequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public RoundRobinStrategy(List<RequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RequestSender createSenderInstance(List<RequestSender> subSenders) {
		return new RequestSenderRoundRobin(subSenders);
	}

	final static class RequestSenderRoundRobin implements RequestSender {
		private int nextSender;
		private List<RequestSender> subSenders;

		public RequestSenderRoundRobin(List<RequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0);
			this.subSenders = senders;
			this.nextSender = 0;
		}


		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender sender = getCurrentSubSender();
			sender.sendRequest(request, timeout, callback);
		}

		private RequestSender getCurrentSubSender() {
			RequestSender currentSender = subSenders.get(nextSender);
			nextSender = (nextSender + 1) % subSenders.size();
			return currentSender;
		}
	}
}
