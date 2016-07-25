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

package io.datakernel.jmx;

import java.util.List;

import static java.util.Collections.singletonList;

final class AttributeNodeForJmxRefreshableStats extends AttributeNodeForJmxStatsAbstract {

	public AttributeNodeForJmxRefreshableStats(String name, ValueFetcher fetcher,
	                                           Class<? extends JmxRefreshableStats<?>> jmxStatsClass,
	                                           List<? extends AttributeNode> subNodes) {
		super(name, fetcher, jmxStatsClass, subNodes);
	}

	@Override
	public Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		return singletonList((JmxRefreshable) fetcher.fetchFrom(source));
	}
}
