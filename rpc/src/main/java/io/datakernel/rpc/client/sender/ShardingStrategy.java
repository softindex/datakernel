package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.hash.HashFunction;

import static io.datakernel.rpc.protocol.RpcMessage.RpcMessageData;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class ShardingStrategy extends RequestSendingStrategyToGroup {
	private final HashFunction<RpcMessageData> hashFunction;

	public ShardingStrategy(HashFunction<RpcMessageData> hashFunction, List<RequestSendingStrategy> subStrategies) {
		super(subStrategies);
		this.hashFunction = checkNotNull(hashFunction);
	}

	public ShardingStrategy(HashFunction<RpcMessageData> hashFunction, List<RequestSendingStrategy> subStrategies,
	                        int minSubStrategiesForCreation) {
		super(subStrategies, minSubStrategiesForCreation);
		this.hashFunction = checkNotNull(hashFunction);
	}

	@Override
	protected RequestSender createSenderInstance(List<RequestSender> subSenders) {
		return new RequestSenderSharding(hashFunction, subSenders);
	}

	final static class RequestSenderSharding implements RequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No senders available");
		private final HashFunction<RpcMessageData> hashFunction;
		private final List<RequestSender> subSenders;

		public RequestSenderSharding(HashFunction<RpcMessageData> hashFunction, List<RequestSender> senders) {
			this.hashFunction = checkNotNull(hashFunction);
			this.subSenders = senders;
		}


		@Override
		public <T extends RpcMessageData> void sendRequest(RpcMessageData request, int timeout,
		                                                              final ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender sender = chooseSender(request);
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
				return;
			}
			callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
		}

		private RequestSender chooseSender(RpcMessageData request) {
			int index = Math.abs(hashFunction.hashCode(request)) % subSenders.size();
			return subSenders.get(index);
		}
	}
}
