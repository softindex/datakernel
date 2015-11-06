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
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.rpc.client.sender.RpcSendersUtils.*;
import static java.util.Arrays.asList;

public abstract class RpcRequestSendingStrategyToGroup implements RpcRequestSendingStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;

	private final int minSubStrategiesForCreation;
	private final List<RpcRequestSendingStrategy> subStrategies;

	RpcRequestSendingStrategyToGroup(List<RpcRequestSendingStrategy> subStrategies) {
		this(subStrategies, MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT);
	}

	RpcRequestSendingStrategyToGroup(List<RpcRequestSendingStrategy> subStrategies, int minSubStrategiesForCreation) {
		checkArgument(minSubStrategiesForCreation > 0, "minSubStrategiesForCreation must be greater than 0");
		this.subStrategies = checkNotNull(subStrategies);
		this.minSubStrategiesForCreation = minSubStrategiesForCreation;
	}

	@Override
	public final List<Optional<RpcRequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public final Optional<RpcRequestSender> create(RpcClientConnectionPool pool) {
		List<Optional<RpcRequestSender>> subSenders = createSubSenders(pool);
		if (countPresentValues(subSenders) >= minSubStrategiesForCreation) {
			return Optional.of(createSenderInstance(filterAbsent(subSenders)));
		} else {
			return Optional.absent();
		}
	}

	protected abstract RpcRequestSender createSenderInstance(List<RpcRequestSender> subSenders);

	private final List<Optional<RpcRequestSender>> createSubSenders(RpcClientConnectionPool pool) {

		assert subStrategies != null;

		List<List<Optional<RpcRequestSender>>> listOfListOfSenders = new ArrayList<>();
		for (RpcRequestSendingStrategy subStrategy : subStrategies) {
			listOfListOfSenders.add(subStrategy.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}
}
