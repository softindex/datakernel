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

import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.cube.bean.TestPubRequest.TestAdvRequest;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.etl.LogDataConsumerSplitter;
import io.datakernel.stream.StreamDataAcceptor;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.keysToMap;
import static java.util.Collections.singleton;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

public class TestAggregatorSplitter extends LogDataConsumerSplitter<TestPubRequest, CubeDiff> {
	private final Cube cube;

	public TestAggregatorSplitter(Cube cube) {
		this.cube = cube;
	}

	public static class AggregationItem {
		// pub
		public int date;
		public int hourOfDay;
		public int pub;

		public final long pubRequests = 1;

		// adv
		public int adv;
		public final long advRequests = 1;

		@Override
		public String toString() {
			return "AggregationItem{date=" + date + ", hourOfDay=" + hourOfDay + ", pub=" + pub + ", pubRequests=" + pubRequests + ", adv=" + adv + ", advRequests=" + advRequests + '}';
		}
	}

	private static final Set<String> PUB_DIMENSIONS = Stream.of("date", "hourOfDay", "pub").collect(toSet());
	private static final Set<String> PUB_METRICS = singleton("pubRequests");
	private static final Set<String> ADV_DIMENSIONS = union(PUB_DIMENSIONS, singleton("adv"));
	private static final Set<String> ADV_METRICS = singleton("advRequests");

	private static <T> Set<T> union(Set<T> a, Set<T> b) {
		Set<T> set = new HashSet<>(a);
		set.addAll(b);
		return set;
	}

	@Override
	protected StreamDataAcceptor<TestPubRequest> createSplitter() {
		return new StreamDataAcceptor<TestPubRequest>() {
			private final AggregationItem outputItem = new AggregationItem();

			private final StreamDataAcceptor<AggregationItem> pubAggregator = addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							keysToMap(PUB_DIMENSIONS.stream(), identity()),
							keysToMap(PUB_METRICS.stream(), identity())));

			private final StreamDataAcceptor<AggregationItem> advAggregator = addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							keysToMap(ADV_DIMENSIONS.stream(), identity()),
							keysToMap(ADV_METRICS.stream(), identity())));

			@SuppressWarnings("ConstantConditions")
			@Override
			public void accept(TestPubRequest pubRequest) {
				outputItem.date = (int) (pubRequest.timestamp / (24 * 60 * 60 * 1000L));
				outputItem.hourOfDay = (byte) ((pubRequest.timestamp / (60 * 60 * 1000L)) % 24);
				outputItem.pub = pubRequest.pub;
				pubAggregator.accept(outputItem);
				for (TestAdvRequest remRequest : pubRequest.advRequests) {
					outputItem.adv = remRequest.adv;
					advAggregator.accept(outputItem);
				}
			}
		};
	}

}
