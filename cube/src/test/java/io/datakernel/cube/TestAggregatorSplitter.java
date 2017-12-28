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

import io.datakernel.aggregation.AggregationUtils;
import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.ot.LogDataConsumerSplitter;
import io.datakernel.stream.StreamDataReceiver;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("unchecked")
public class TestAggregatorSplitter extends LogDataConsumerSplitter<TestPubRequest, CubeDiff> {
	private final Cube cube;

	public TestAggregatorSplitter(Eventloop eventloop, Cube cube) {
		super(eventloop);
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
	protected StreamDataReceiver<TestPubRequest> createSplitter() {
		return new StreamDataReceiver<TestPubRequest>() {
			private final AggregationItem outputItem = new AggregationItem();

			private final StreamDataReceiver<AggregationItem> pubAggregator = addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							AggregationUtils.streamToLinkedMap(PUB_DIMENSIONS.stream(), o -> o),
							AggregationUtils.streamToLinkedMap(PUB_METRICS.stream(), o -> o)));

			private final StreamDataReceiver<AggregationItem> advAggregator = addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							AggregationUtils.streamToLinkedMap(ADV_DIMENSIONS.stream(), o -> o),
							AggregationUtils.streamToLinkedMap(ADV_METRICS.stream(), o -> o)));

			@Override
			public void onData(TestPubRequest pubRequest) {
				outputItem.date = (int) (pubRequest.timestamp / (24 * 60 * 60 * 1000L));
				outputItem.hourOfDay = (byte) ((pubRequest.timestamp / (60 * 60 * 1000L)) % 24);
				outputItem.pub = pubRequest.pub;
				pubAggregator.onData(outputItem);
				for (TestPubRequest.TestAdvRequest remRequest : pubRequest.advRequests) {
					outputItem.adv = remRequest.adv;
					advAggregator.onData(outputItem);
				}
			}
		};
	}

}
