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

import com.google.common.base.Optional;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

public final class StrategyTypeDispatching extends AbstractRequestSendingStrategy implements SingleSenderStrategy {

	public enum Importance {
		MANDATORY, OPTIONAL
	}

	private Map<Class<? extends RpcMessage.RpcMessageData>, DataTypeSpecifications> dataTypeToSpecification;
	private SingleSenderStrategy defaultSendingStrategy;

	StrategyTypeDispatching() {
		dataTypeToSpecification = new HashMap<>();
		defaultSendingStrategy = null;
	}

	@Override
	protected List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public Optional<RequestSender> create(RpcClientConnectionPool pool) {
		Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> dataTypeToSender = new HashMap<>();
		for (Class<? extends RpcMessage.RpcMessageData> dataType : dataTypeToSpecification.keySet()) {
			DataTypeSpecifications specs = dataTypeToSpecification.get(dataType);
			Optional<RequestSender> sender = specs.getStrategy().create(pool);
			if (sender.isPresent()) {
				dataTypeToSender.put(dataType, sender.get());
			} else if (specs.getImportance() == Importance.MANDATORY) {
				return Optional.absent();
			}
		}
		Optional<RequestSender> defaultSender =
				defaultSendingStrategy != null ? defaultSendingStrategy.create(pool) : Optional.<RequestSender>absent();
		return Optional.<RequestSender>of(new RequestSenderTypeDispatcher(dataTypeToSender, defaultSender.orNull()));
	}

	public StrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                  SingleSenderStrategy strategy) {
		return on(dataType, strategy, Importance.MANDATORY);
	}

	private StrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                                SingleSenderStrategy strategy, Importance importance) {
		checkNotNull(dataType);
		checkNotNull(strategy);
		checkNotNull(importance);
		dataTypeToSpecification.put(dataType, new DataTypeSpecifications(strategy, importance));
		return this;
	}

	public StrategyTypeDispatching onDefault(SingleSenderStrategy strategy) {
		checkState(defaultSendingStrategy == null, "Default Strategy is already set");
		defaultSendingStrategy = strategy;
		return this;
	}

	private static final class DataTypeSpecifications {
		private final SingleSenderStrategy strategy;
		private final Importance importance;

		public DataTypeSpecifications(SingleSenderStrategy strategy, Importance importance) {
			this.strategy = checkNotNull(strategy);
			this.importance = checkNotNull(importance);
		}

		public SingleSenderStrategy getStrategy() {
			return strategy;
		}

		public Importance getImportance() {
			return importance;
		}
	}


	final static class RequestSenderTypeDispatcher implements RequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No active senders available");

		private Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> dataTypeToSender;
		private RequestSender defaultSender;

		public RequestSenderTypeDispatcher(Map<Class<? extends RpcMessage.RpcMessageData>, RequestSender> dataTypeToSender,
		                                   RequestSender defaultSender) {
			checkNotNull(dataTypeToSender);

			this.dataTypeToSender = dataTypeToSender;
			this.defaultSender = defaultSender;
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request,int timeout, ResultCallback<T> callback) {
			checkNotNull(callback);

			RequestSender specifiedSender = dataTypeToSender.get(request.getClass());
			RequestSender sender = specifiedSender != null ? specifiedSender : defaultSender;
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
			} else {
				callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}
	}
}
