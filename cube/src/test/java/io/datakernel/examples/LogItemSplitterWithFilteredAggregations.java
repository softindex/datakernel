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

package io.datakernel.examples;

import io.datakernel.aggregation_db.AggregationQuery;
import io.datakernel.cube.AggregatorSplitter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamDataReceiver;

import static io.datakernel.examples.CubeExampleWithFilteredAggregations.ImpressionType.*;

/**
 * Defines the logic for processing facts (log items) and splitting the retrieved data across aggregators.
 * This logic may include converting or reformatting data.
 */
public class LogItemSplitterWithFilteredAggregations extends AggregatorSplitter<LogItemWithFilteredAggregations> {
	private static final Factory<LogItemWithFilteredAggregations> FACTORY = new Factory<LogItemWithFilteredAggregations>() {
		@Override
		public AggregatorSplitter<LogItemWithFilteredAggregations> create(Eventloop eventloop) {
			return new LogItemSplitterWithFilteredAggregations(eventloop);
		}
	};

	public LogItemSplitterWithFilteredAggregations(Eventloop eventloop) {
		super(eventloop);
	}

	public static Factory<LogItemWithFilteredAggregations> factory() {
		return FACTORY;
	}

	private StreamDataReceiver<LogItemWithFilteredAggregations> logItemAggregator;
	private StreamDataReceiver<LogItemWithFilteredAggregations> logItemBannerAggregator;
	private StreamDataReceiver<LogItemWithFilteredAggregations> logItemVideoAggregator;
	private StreamDataReceiver<LogItemWithFilteredAggregations> logItemNativeAggregator;
	private StreamDataReceiver<LogItemWithFilteredAggregations> logItemMayAggregator;
	private StreamDataReceiver<LogItemWithFilteredAggregations> logItemMayBannerAggregator;
	private StreamDataReceiver<LogItemWithFilteredAggregations> logItemJuneAggregator;

	@Override
	protected void addOutputs() {
		logItemAggregator = addOutput(LogItemWithFilteredAggregations.class,
				LogItemWithFilteredAggregations.ALL_DIMENSIONS, LogItemWithFilteredAggregations.MEASURES);

		logItemBannerAggregator = addOutput(LogItemWithFilteredAggregations.class,
				LogItemWithFilteredAggregations.ALL_DIMENSIONS_EXCEPT_TYPE, LogItemWithFilteredAggregations.MEASURES,
				new AggregationQuery.QueryPredicates().eq("impressionType", BANNER));
		logItemVideoAggregator = addOutput(LogItemWithFilteredAggregations.class,
				LogItemWithFilteredAggregations.ALL_DIMENSIONS_EXCEPT_TYPE, LogItemWithFilteredAggregations.MEASURES,
				new AggregationQuery.QueryPredicates().eq("impressionType", VIDEO));
		logItemNativeAggregator = addOutput(LogItemWithFilteredAggregations.class,
				LogItemWithFilteredAggregations.ALL_DIMENSIONS_EXCEPT_TYPE, LogItemWithFilteredAggregations.MEASURES,
				new AggregationQuery.QueryPredicates().eq("impressionType", NATIVE));

		logItemMayAggregator = addOutput(LogItemWithFilteredAggregations.class,
				LogItemWithFilteredAggregations.ALL_DIMENSIONS_EXCEPT_TYPE, LogItemWithFilteredAggregations.MEASURES,
				new AggregationQuery.QueryPredicates().between("date", 16570, 16585));
		logItemJuneAggregator = addOutput(LogItemWithFilteredAggregations.class,
				LogItemWithFilteredAggregations.ALL_DIMENSIONS_EXCEPT_TYPE, LogItemWithFilteredAggregations.MEASURES,
				new AggregationQuery.QueryPredicates().between("date", 16585, 16600));

		logItemMayBannerAggregator = addOutput(LogItemWithFilteredAggregations.class,
				LogItemWithFilteredAggregations.ALL_DIMENSIONS_EXCEPT_TYPE, LogItemWithFilteredAggregations.MEASURES,
				new AggregationQuery.QueryPredicates()
						.between("date", 16570, 16585)
						.eq("impressionType", BANNER));
	}

	@Override
	protected void processItem(LogItemWithFilteredAggregations item) {
		logItemAggregator.onData(item);

		switch (item.impressionType) {
			case BANNER:
				logItemBannerAggregator.onData(item);
				break;
			case VIDEO:
				logItemVideoAggregator.onData(item);
				break;
			case NATIVE:
				logItemNativeAggregator.onData(item);
				break;
		}

		if (item.date >= 16570 && item.date < 16585) {
			logItemMayAggregator.onData(item);
			if (item.impressionType == BANNER)
				logItemMayBannerAggregator.onData(item);
		} else if (item.date >= 16585 && item.date < 16600)
			logItemJuneAggregator.onData(item);
	}
}
