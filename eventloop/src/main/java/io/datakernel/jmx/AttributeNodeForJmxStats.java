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

final class AttributeNodeForJmxStats extends AttributeNodeForJmxStatsAbstract {
	private final Class<? extends JmxStats<?>> jmxStatsClass;

	public AttributeNodeForJmxStats(String name, String description,
	                                ValueFetcher fetcher, Class<? extends JmxStats<?>> jmxStatsClass,
	                                List<? extends AttributeNode> subNodes) {
		super(name, description, fetcher, jmxStatsClass, subNodes);
		this.jmxStatsClass = jmxStatsClass;
	}

	@Override
	public Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		return null;
	}

	@Override
	protected AttributeNode recreate(List<AttributeNode> filteredNodes) {
		return new AttributeNodeForJmxStats(name, description, fetcher, jmxStatsClass, filteredNodes);
	}
}
