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
import io.datakernel.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class RpcStrategyRandomSamplingTest {
	// we do not care about pool in this test
	private static final RpcClientConnectionPool POOL = null;

	@Test
	public void distributesRequestsProperlyAccordingToWeights() {
		// init subStrategies
		RpcSenderStub sender_1 = new RpcSenderStub();
		RpcStrategyStub strategy_1 = new RpcStrategyStub(sender_1);
		int strategy_1_weight = 2;

		RpcSenderStub sender_2 = new RpcSenderStub();
		RpcStrategyStub strategy_2 = new RpcStrategyStub(sender_2);
		int strategy_2_weight = 3;

		RpcSenderStub sender_3 = new RpcSenderStub();
		RpcStrategyStub strategy_3 = new RpcStrategyStub(sender_3);
		int strategy_3_weight = 7;

		// init RandomSamplingStrategy
		RpcStrategyRandomSampling randomSamplingStrategy =
				new RpcStrategyRandomSampling()
						.add(strategy_1_weight, strategy_1)
						.add(strategy_2_weight, strategy_2)
						.add(strategy_3_weight, strategy_3);

		// make requests
		RpcSender sender = randomSamplingStrategy.createSender(POOL);
		int totalRequests = 10000;
		for (int i = 0; i < totalRequests; i++) {
			sender.sendRequest(new Object(), 0, null);
		}

		// check
		assertEquals(totalRequests, sender_1.getRequests() + sender_2.getRequests() + sender_3.getRequests());

		int totalWeight = strategy_1_weight + strategy_2_weight + strategy_3_weight;
		double acceptableError = totalRequests / 50.0;

		double sender_1_expectedRequests = totalRequests * strategy_1_weight / (double) totalWeight;
		assertEquals(sender_1_expectedRequests, sender_1.getRequests(), acceptableError);

		double sender_2_expectedRequests = totalRequests * strategy_2_weight / (double) totalWeight;
		assertEquals(sender_2_expectedRequests, sender_2.getRequests(), acceptableError);

		double sender_3_expectedRequests = totalRequests * strategy_3_weight / (double) totalWeight;
		assertEquals(sender_3_expectedRequests, sender_3.getRequests(), acceptableError);

	}

	@Test
	public void doesNotSendRequestsToStrategiesWithWeightZero() {
		// init subStrategies
		RpcSenderStub sender_1 = new RpcSenderStub();
		RpcStrategyStub strategy_1 = new RpcStrategyStub(sender_1);
		int strategy_1_weight = 2;

		RpcSenderStub sender_2 = new RpcSenderStub();
		RpcStrategyStub strategy_2 = new RpcStrategyStub(sender_2);
		int zero_weight = 0;

		RpcSenderStub sender_3 = new RpcSenderStub();
		RpcStrategyStub strategy_3 = new RpcStrategyStub(sender_3);
		int strategy_3_weight = 8;

		// init RandomSamplingStrategy
		RpcStrategyRandomSampling randomSamplingStrategy =
				new RpcStrategyRandomSampling()
						.add(strategy_1_weight, strategy_1)
						.add(zero_weight, strategy_2)
						.add(strategy_3_weight, strategy_3);

		// make requests
		RpcSender sender = randomSamplingStrategy.createSender(POOL);
		int totalRequests = 10000;
		for (int i = 0; i < totalRequests; i++) {
			sender.sendRequest(null, 0, null);
		}

		// check
		assertEquals(totalRequests, sender_1.getRequests() + sender_2.getRequests() + sender_3.getRequests());

		assertEquals(0, sender_2.getRequests());

	}

	private static final class RpcStrategyStub implements RpcStrategy {
		private final RpcSender sender;

		public RpcStrategyStub(RpcSender sender) {
			this.sender = sender;
		}

		@Override
		public Set<InetSocketAddress> getAddresses() {
			return new HashSet<>();
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return sender;
		}
	}

}
