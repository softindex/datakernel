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

package io.datakernel.cube;

import io.datakernel.serializer.annotations.Serialize;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;

public class TestPubRequest {
	private final static Random rand = new Random();

	public static class TestAdvRequest {
		@Serialize(order = 0)
		public int adv;

		public TestAdvRequest() {
		}

		public TestAdvRequest(int adv) {
			this.adv = adv;
		}

		@Override
		public String toString() {
			return "TestAdvRequest{adv=" + adv + '}';
		}
	}

	@Serialize(order = 0)
	public long timestamp;
	@Serialize(order = 1)
	public int pub;

	@Serialize(order = 2)
	public List<TestAdvRequest> advRequests = new ArrayList<>();

	public TestPubRequest() {
	}

	public TestPubRequest(int timestamp, int pub, List<TestAdvRequest> advRequests) {
		this.timestamp = timestamp;
		this.pub = pub;
		this.advRequests = advRequests;
	}

	public static int randInt(int min, int max) {
		return rand.nextInt((max - min) + 1) + min;
	}

	public static TestPubRequest randomPubRequest() {
		TestPubRequest randomRequest = new TestPubRequest();
		randomRequest.timestamp = randInt(1, 10_000);
		randomRequest.pub = randInt(1, 10_000);

		List<TestAdvRequest> advRequests = new ArrayList<>();
		for (int i = 0; i < randInt(1, 20); ++i) {
			advRequests.add(new TestAdvRequest(randInt(1, 10_000)));
		}
		randomRequest.advRequests = advRequests;

		return randomRequest;
	}

	@Override
	public String toString() {
		return "TestPubRequest{timestamp=" + timestamp + ", pub=" + pub + ", advRequests=" + advRequests + '}';
	}

	public static final List<String> DIMENSIONS = asList("adv", "pub");

	public static final List<String> METRICS = asList("advRequests", "pubRequests");
}
