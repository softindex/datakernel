/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

public final class RpcStrategyTypeDispatching implements RpcRequestSendingStrategy {
	private Map<Class<?>, RpcRequestSendingStrategy> dataTypeToStrategy;
	private RpcRequestSendingStrategy defaultStrategy;

	RpcStrategyTypeDispatching() {
		dataTypeToStrategy = new HashMap<>();
		defaultStrategy = null;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		HashSet<InetSocketAddress> result = new HashSet<>();
		for (RpcRequestSendingStrategy strategy : dataTypeToStrategy.values()) {
			result.addAll(strategy.getAddresses());
		}
		return result;
	}

	@Override
	public RpcRequestSender createSender(RpcClientConnectionPool pool) {
		HashMap<Class<?>, RpcRequestSender> dataTypeToSender = new HashMap<>();
		for (Class<?> dataType : dataTypeToStrategy.keySet()) {
			RpcRequestSendingStrategy strategy = dataTypeToStrategy.get(dataType);
			RpcRequestSender sender = strategy.createSender(pool);
			if (sender == null)
				return null;
			dataTypeToSender.put(dataType, sender);
		}
		RpcRequestSender defaultSender = null;
		if (defaultStrategy != null) {
			defaultSender = defaultStrategy.createSender(pool);
			if (defaultSender == null)
				return null;
		}
		return new RequestSenderTypeDispatcher(dataTypeToSender, defaultSender);
	}

	public RpcStrategyTypeDispatching on(Class<?> dataType,
	                                     RpcRequestSendingStrategy strategy) {
		checkNotNull(dataType);
		checkNotNull(strategy);
		checkState(!dataTypeToStrategy.containsKey(dataType),
				"Strategy for type " + dataType.toString() + " is already set");
		dataTypeToStrategy.put(dataType, strategy);
		return this;
	}

	public RpcStrategyTypeDispatching onDefault(RpcRequestSendingStrategy strategy) {
		checkState(defaultStrategy == null, "Default Strategy is already set");
		defaultStrategy = strategy;
		return this;
	}

	final static class RequestSenderTypeDispatcher implements RpcRequestSender {
		@SuppressWarnings("ThrowableInstanceNeverThrown")
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No active senders available");

		private final HashMap<Class<?>, RpcRequestSender> dataTypeToSender;
		private final RpcRequestSender defaultSender;

		public RequestSenderTypeDispatcher(HashMap<Class<?>, RpcRequestSender> dataTypeToSender,
		                                   RpcRequestSender defaultSender) {
			checkNotNull(dataTypeToSender);

			this.dataTypeToSender = dataTypeToSender;
			this.defaultSender = defaultSender;
		}

		@Override
		public <I, O> void sendRequest(I request, int timeout, ResultCallback<O> callback) {
			RpcRequestSender specifiedSender = dataTypeToSender.get(request.getClass());
			RpcRequestSender sender = specifiedSender != null ? specifiedSender : defaultSender;
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
			} else {
				callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}
	}
}
