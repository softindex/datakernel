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

import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.rpc.client.sender.RpcSendersUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public abstract class RpcRequestSendingStrategyToGroup implements RpcRequestSendingStrategy {
	private static final int MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT = 1;

	private final List<RpcRequestSendingStrategy> subStrategies;
	private int minSubStrategiesForCreation;

	RpcRequestSendingStrategyToGroup(List<RpcRequestSendingStrategy> subStrategies) {
		this.subStrategies = checkNotNull(subStrategies);
		this.minSubStrategiesForCreation = MIN_SUB_STRATEGIES_FOR_CREATION_DEFAULT;
	}

	protected final void setMinSubStrategiesForCreation(int minSubStrategiesForCreation) {
		checkArgument(minSubStrategiesForCreation > 0, "minSubStrategiesForCreation must be greater than 0");
		this.minSubStrategiesForCreation = minSubStrategiesForCreation;
	}

	@Override
	public final List<RpcRequestSenderHolder> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public final RpcRequestSenderHolder create(RpcClientConnectionPool pool) {
		List<RpcRequestSenderHolder> subSenders = createSubSenders(pool);
		if (countPresentSenders(subSenders) >= minSubStrategiesForCreation) {
			return RpcRequestSenderHolder.of(createSenderInstance(filterAbsent(subSenders)));
		} else {
			return RpcRequestSenderHolder.absent();
		}
	}

	protected abstract RpcRequestSender createSenderInstance(List<RpcRequestSender> subSenders);

	private final List<RpcRequestSenderHolder> createSubSenders(RpcClientConnectionPool pool) {

		assert subStrategies != null;

		List<List<RpcRequestSenderHolder>> listOfListOfSenders = new ArrayList<>();
		for (RpcRequestSendingStrategy subStrategy : subStrategies) {
			listOfListOfSenders.add(subStrategy.createAsList(pool));
		}
		return flatten(listOfListOfSenders);
	}
}
