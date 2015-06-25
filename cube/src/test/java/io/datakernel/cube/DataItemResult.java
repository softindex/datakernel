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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static io.datakernel.cube.MeasureType.SUM_LONG;

public class DataItemResult {
	public int key1;
	public int key2;

	public long metric1;
	public long metric2;
	public long metric3;

	public DataItemResult() {
	}

	public DataItemResult(int key1, int key2, long metric1, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	public static final Map<String, Class<?>> DIMENSIONS = ImmutableMap.<String, Class<?>>builder()
			.put("key1", int.class)
			.put("key2", int.class)
			.build();

	public static final Map<String, MeasureType> METRICS = ImmutableMap.<String, MeasureType>builder()
			.put("metric1", SUM_LONG)
			.put("metric2", SUM_LONG)
			.put("metric3", SUM_LONG)
			.build();

	@Override
	public boolean equals(Object o) {
		DataItemResult that = (DataItemResult) o;

		if (key1 != that.key1) return false;
		if (key2 != that.key2) return false;
		if (metric1 != that.metric1) return false;
		if (metric2 != that.metric2) return false;
		if (metric3 != that.metric3) return false;

		return true;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("key1", key1)
				.add("key2", key2)
				.add("metric1", metric1)
				.add("metric2", metric2)
				.add("metric3", metric3)
				.toString();
	}
}
