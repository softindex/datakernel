package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.util.ArrayList;
import java.util.List;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

abstract class RequestSendingStrategyToGroup extends AbstractRequestSendingStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;

	private final int minSubStrategiesForCreation;
	private final List<RequestSendingStrategy> subStrategies;

	RequestSendingStrategyToGroup(List<RequestSendingStrategy> subStrategies) {
		this(subStrategies, MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT);
	}

	RequestSendingStrategyToGroup(List<RequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		checkArgument(minSubStrategiesForCreation > 0, "minSubStrategiesForCreation must be greater than 0");
		this.subStrategies = checkNotNull(subStrategies);
		this.minSubStrategiesForCreation = minSubStrategiesForCreation;
	}

	@Override
	protected final List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public final Optional<RequestSender> create(RpcClientConnectionPool pool) {
		List<Optional<RequestSender>> subSenders = createSubSenders(pool);
		if (countPresentValues(subSenders) >= minSubStrategiesForCreation) {
			return Optional.of(createSenderInstance(filterAbsent(subSenders)));
		} else {
			return Optional.absent();
		}
	}

	protected abstract RequestSender createSenderInstance(List<RequestSender> subSenders);

	private final List<Optional<RequestSender>> createSubSenders(RpcClientConnectionPool pool) {

		assert subStrategies != null;

		List<List<Optional<RequestSender>>> listOfListOfSenders = new ArrayList<>();
		for (RequestSendingStrategy subStrategy : subStrategies) {
			AbstractRequestSendingStrategy abstractSubSendingStrategy = (AbstractRequestSendingStrategy)subStrategy;
			listOfListOfSenders.add(abstractSubSendingStrategy.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}

	static <T> List<T> flatten(List<List<T>> listOfList) {
		List<T> flatList = new ArrayList<>();
		for (List<T> list : listOfList) {
			for (T element : list) {
				flatList.add(element);
			}
		}
		return flatList;
	}

	static <T> int countPresentValues(List<Optional<T>> values) {
		int counter = 0;
		for (Optional<T> value : values) {
			if (value.isPresent()) {
				++counter;
			}
		}
		return counter;
	}

	static <T> List<T> filterAbsent(List<Optional<T>> list) {
		List<T> filtered = new ArrayList<>();
		for (Optional<T> value : list) {
			if (value.isPresent()) {
				filtered.add(value.get());
			}
		}
		return filtered;
	}
}
