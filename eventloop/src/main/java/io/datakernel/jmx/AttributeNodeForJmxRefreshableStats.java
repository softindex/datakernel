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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

final class AttributeNodeForJmxRefreshableStats extends AttributeNodeForJmxStatsAbstract {
	// TODO(vmykhalko): refactor: this class uses fields only for recreate(), maybe they should be passed in recreate method too?
	private final Class<? extends JmxRefreshableStats<?>> jmxStatsClass;
	private final Cache<Map<String, Object>> cache;

	public AttributeNodeForJmxRefreshableStats(String name, String description, boolean included, ValueFetcher fetcher,
	                                           Class<? extends JmxRefreshableStats<?>> jmxStatsClass,
	                                           List<? extends AttributeNode> subNodes,
	                                           Cache<Map<String, Object>> cache) {
		super(name, description, included, fetcher, jmxStatsClass, subNodes, cache);
		this.jmxStatsClass = jmxStatsClass;
		this.cache = cache;
	}

	@Override
	public Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		JmxRefreshable jmxRefreshable = (JmxRefreshable) fetcher.fetchFrom(source);
		return jmxRefreshable != null ? singletonList(jmxRefreshable) : Collections.<JmxRefreshable>emptyList();
	}

	@Override
	protected AttributeNode recreate(List<? extends AttributeNode> subNodes, boolean visible) {
		return new AttributeNodeForJmxRefreshableStats(
				name, description, visible, fetcher, jmxStatsClass, subNodes, cache);
	}
}
