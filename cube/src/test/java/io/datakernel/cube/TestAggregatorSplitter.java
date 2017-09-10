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

import com.google.common.base.Functions;
import io.datakernel.async.StagesAccumulator;
import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.ot.LogDataConsumerSplitter;
import io.datakernel.stream.StreamDataReceiver;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Maps.asMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;

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

	private static final Set<String> PUB_DIMENSIONS = newHashSet("date", "hourOfDay", "pub");
	private static final Set<String> PUB_METRICS = newHashSet("pubRequests");
	private static final Set<String> ADV_DIMENSIONS = newHashSet(union(PUB_DIMENSIONS, newHashSet("adv")));
	private static final Set<String> ADV_METRICS = newHashSet("advRequests");

	@Override
	protected StreamDataReceiver<TestPubRequest> createSplitter() {
		return new StreamDataReceiver<TestPubRequest>() {
			private final AggregationItem outputItem = new AggregationItem();

			private final StreamDataReceiver<AggregationItem> pubAggregator = addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							asMap(PUB_DIMENSIONS, Functions.<String>identity()),
							asMap(PUB_METRICS, Functions.<String>identity())));

			private final StreamDataReceiver<AggregationItem> advAggregator = addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							asMap(ADV_DIMENSIONS, Functions.<String>identity()),
							asMap(ADV_METRICS, Functions.<String>identity())));

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
