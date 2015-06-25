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

package io.datakernel.example;

import io.datakernel.cube.AggregatorSplitter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamDataReceiver;

/*
Defines the logic for processing facts (log items) and splitting the retrieved data across aggregators.
This logic may include converting or reformatting data.
 */
public class LogItemSplitter extends AggregatorSplitter<LogItem> {
	private static final AggregatorSplitter.Factory<LogItem> FACTORY = new Factory<LogItem>() {
		@Override
		public AggregatorSplitter<LogItem> create(Eventloop eventloop) {
			return new LogItemSplitter(eventloop);
		}
	};

	public LogItemSplitter(Eventloop eventloop) {
		super(eventloop);
	}

	public static Factory<LogItem> factory() {
		return FACTORY;
	}

	private StreamDataReceiver<LogItem> logItemAggregator;

	@Override
	protected void addOutputs() {
		logItemAggregator = addOutput(LogItem.class, LogItem.DIMENSIONS, LogItem.MEASURES);
	}

	@Override
	public void onData(LogItem item) {
		logItemAggregator.onData(item);
	}
}
