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

public final class StrategyTypeDispatching extends AbstractRequestSendingStrategy {

	public enum Importance {
		MANDATORY, OPTIONAL
	}

	private Map<Class<? extends RpcMessage.RpcMessageData>, DataTypeSpecifications> dataTypeToSpecification;
	private RequestSendingStrategy defaultSendingStrategy;

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



	// this group of similar methods was created to enable type checking
	// and ensure that servers() result can't be applied in put() method as second argument
	// because in this case we don't know how to choose one of them to send request

	public StrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                         RequestSendingStrategyToGroup strategy) {
		return onCommon(dataType, strategy, Importance.MANDATORY);
	}

	public StrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                         StrategySingleServer strategy) {
		return onCommon(dataType, strategy, Importance.MANDATORY);
	}

	public StrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                          StrategyRendezvousHashing strategy) {
		return onCommon(dataType, strategy, Importance.MANDATORY);
	}

	public StrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                         StrategyTypeDispatching strategy) {
		return onCommon(dataType, strategy, Importance.MANDATORY);
	}

	public StrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                  StrategySharding strategy) {
		return onCommon(dataType, strategy, Importance.MANDATORY);
	}

	private StrategyTypeDispatching onCommon(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                                RequestSendingStrategy strategy, Importance importance) {
		checkNotNull(dataType);
		checkNotNull(strategy);
		checkNotNull(importance);
		dataTypeToSpecification.put(dataType, new DataTypeSpecifications(strategy, importance));
		return this;
	}



	// this group of similar methods was created to enable type checking
	// and ensure that servers() result can't be applied in put() method as second argument
	// because in this case we don't know how to choose one of them to send request

	public StrategyTypeDispatching onDefault(RequestSendingStrategyToGroup strategy) {
		return onDefaultCommon(strategy);
	}

	public StrategyTypeDispatching onDefault(StrategySingleServer strategy) {
		return onDefaultCommon(strategy);
	}

	public StrategyTypeDispatching onDefault(StrategyRendezvousHashing strategy) {
		return onDefaultCommon(strategy);
	}

	public StrategyTypeDispatching onDefault(StrategyTypeDispatching strategy) {
		return onDefaultCommon(strategy);
	}
	public StrategyTypeDispatching onDefault(StrategySharding strategy) {
		return onDefaultCommon(strategy);
	}

	private StrategyTypeDispatching onDefaultCommon(RequestSendingStrategy strategy) {
		checkState(defaultSendingStrategy == null, "Default Strategy is already set");
		defaultSendingStrategy = strategy;
		return this;
	}

	private static final class DataTypeSpecifications {
		private final RequestSendingStrategy strategy;
		private final Importance importance;

		public DataTypeSpecifications(RequestSendingStrategy strategy, Importance importance) {
			this.strategy = checkNotNull(strategy);
			this.importance = checkNotNull(importance);
		}

		public RequestSendingStrategy getStrategy() {
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
