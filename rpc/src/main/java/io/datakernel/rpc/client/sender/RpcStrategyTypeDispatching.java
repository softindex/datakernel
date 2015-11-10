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

public final class RpcStrategyTypeDispatching implements RpcRequestSendingStrategy, RpcSingleSenderStrategy {
	private Map<Class<? extends RpcMessage.RpcMessageData>, StrategySpecifications> dataTypeToSpecification;
	private StrategySpecifications defaultStrategySpecification;

	RpcStrategyTypeDispatching() {
		dataTypeToSpecification = new HashMap<>();
		defaultStrategySpecification = null;
	}

	@Override
	public List<Optional<RpcRequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public Optional<RpcRequestSender> create(RpcClientConnectionPool pool) {
		HashMap<Class<? extends RpcMessage.RpcMessageData>, RpcRequestSender> dataTypeToSender = new HashMap<>();
		for (Class<? extends RpcMessage.RpcMessageData> dataType : dataTypeToSpecification.keySet()) {
			StrategySpecifications specs = dataTypeToSpecification.get(dataType);
			Optional<RpcRequestSender> sender = specs.getStrategy().create(pool);
			if (sender.isPresent()) {
				dataTypeToSender.put(dataType, sender.get());
			} else if (specs.isCrucialForActivation()) {
				return Optional.absent();
			}
		}
		RpcRequestSender defaultSender = null;
		if (defaultStrategySpecification != null) {
			defaultSender = defaultStrategySpecification.getStrategy().create(pool).orNull();
			if (defaultSender == null && defaultStrategySpecification.isCrucialForActivation()) {
				return Optional.absent();
			}
		}
		return Optional.<RpcRequestSender>of(new RequestSenderTypeDispatcher(dataTypeToSender, defaultSender));
	}

	public RpcStrategySpecsSetting on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                  RpcSingleSenderStrategy strategy) {
		checkNotNull(dataType);
		checkNotNull(strategy);
		checkState(!dataTypeToSpecification.containsKey(dataType),
				"Strategy for type " + dataType.toString() + " is already set");  // TODO (vmykhalko): maybe it shouldn't throw exception in this case?
		dataTypeToSpecification.put(dataType, new StrategySpecifications(strategy, false));
		return new RpcTypeStrategySpecsHelper(dataType);
	}

	public RpcStrategySpecsSetting onDefault(RpcSingleSenderStrategy strategy) {
		checkState(defaultStrategySpecification == null, "Default Strategy is already set");
		defaultStrategySpecification = new StrategySpecifications(strategy, false);
		return new RpcDefaultStrategySpecsHelper();
	}

	public abstract class RpcStrategySpecsSetting implements RpcRequestSendingStrategy, RpcSingleSenderStrategy {
		private RpcStrategySpecsSetting() {

		}

		@Override
		public List<Optional<RpcRequestSender>> createAsList(RpcClientConnectionPool pool) {
			return RpcStrategyTypeDispatching.this.createAsList(pool);
		}

		@Override
		public Optional<RpcRequestSender> create(RpcClientConnectionPool pool) {
			return RpcStrategyTypeDispatching.this.create(pool);
		}

		public RpcStrategySpecsSetting on(Class<? extends RpcMessage.RpcMessageData> dataType,
		                                  RpcSingleSenderStrategy strategy) {
			return RpcStrategyTypeDispatching.this.on(dataType, strategy);
		}

		public RpcStrategySpecsSetting onDefault(RpcSingleSenderStrategy strategy) {
			return RpcStrategyTypeDispatching.this.onDefault(strategy);
		}

		public abstract RpcStrategyTypeDispatching crucialForActivation(boolean isCrucial);
	}

	private final class RpcTypeStrategySpecsHelper extends RpcStrategySpecsSetting {
		private final Class<? extends RpcMessage.RpcMessageData> dataType;

		private RpcTypeStrategySpecsHelper(Class<? extends RpcMessage.RpcMessageData> dataType) {
			this.dataType = dataType;
		}

		@Override
		public RpcStrategyTypeDispatching crucialForActivation(boolean isCrucial) {
			StrategySpecifications specs = checkNotNull(dataTypeToSpecification.get(dataType));
			dataTypeToSpecification.put(dataType, new StrategySpecifications(specs.getStrategy(), isCrucial));
			return RpcStrategyTypeDispatching.this;
		}
	}

	private final class RpcDefaultStrategySpecsHelper extends RpcStrategySpecsSetting {

		private RpcDefaultStrategySpecsHelper() {

		}

		@Override
		public RpcStrategyTypeDispatching crucialForActivation(boolean isCrucial) {
			checkNotNull(defaultStrategySpecification);
			defaultStrategySpecification
					= new StrategySpecifications(defaultStrategySpecification.getStrategy(), isCrucial);
			return RpcStrategyTypeDispatching.this;
		}
	}

	private static final class StrategySpecifications {
		private final RpcSingleSenderStrategy strategy;
		private final boolean crucialForActivation;

		public StrategySpecifications(RpcSingleSenderStrategy strategy, boolean crucialForActivation) {
			this.strategy = strategy;
			this.crucialForActivation = crucialForActivation;
		}

		public RpcSingleSenderStrategy getStrategy() {
			return strategy;
		}

		public boolean isCrucialForActivation() {
			return crucialForActivation;
		}
	}

	final static class RequestSenderTypeDispatcher implements RpcRequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No active senders available");

		private final HashMap<Class<? extends RpcMessage.RpcMessageData>, RpcRequestSender> dataTypeToSender;
		private final RpcRequestSender defaultSender;

		public RequestSenderTypeDispatcher(HashMap<Class<? extends RpcMessage.RpcMessageData>, RpcRequestSender> dataTypeToSender,
		                                   RpcRequestSender defaultSender) {
			checkNotNull(dataTypeToSender);

			this.dataTypeToSender = dataTypeToSender;
			this.defaultSender = defaultSender;
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {
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
