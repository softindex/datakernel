package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class StrategyFirstAvailable extends RequestSendingStrategyToGroup implements SingleSenderStrategy{

	public StrategyFirstAvailable(List<RequestSendingStrategy> subStrategies) {
		super(subStrategies);
	}

	public StrategyFirstAvailable(List<RequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
	}

	@Override
	protected RequestSender createSenderInstance(List<RequestSender> subSenders) {
		return new RequestSenderToFirst(subSenders);
	}

	final static class RequestSenderToFirst implements RequestSender{
		private final RequestSender first;

		public RequestSenderToFirst(List<RequestSender> senders) {
			checkArgument(senders != null && senders.size() > 0);
			this.first = checkNotNull(senders.get(0));
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {
			checkNotNull(callback);

			first.sendRequest(request, timeout, callback);
		}
	}
}
