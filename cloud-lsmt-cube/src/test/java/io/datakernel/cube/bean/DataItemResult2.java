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

package io.datakernel.cube.bean;

import io.datakernel.aggregation.measure.Measure;

import java.util.Map;
import java.util.stream.Stream;

import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.common.collection.CollectionUtils.keysToMap;

public class DataItemResult2 {
	public int key2;

	public long metric1;
	public long metric2;
	public long metric3;

	public DataItemResult2() {
	}

	public DataItemResult2(int key2, long metric1, long metric2, long metric3) {
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	public static final Map<String, Class<?>> DIMENSIONS =
			keysToMap(Stream.of("key2"), k -> int.class);

	public static final Map<String, Measure> METRICS =
			keysToMap(Stream.of("metric1", "metric2", "metric3"), k -> sum(ofLong()));

	@Override
	public boolean equals(Object o) {
		DataItemResult2 that = (DataItemResult2) o;

		if (key2 != that.key2) return false;
		if (metric1 != that.metric1) return false;
		if (metric2 != that.metric2) return false;
		if (metric3 != that.metric3) return false;

		return true;
	}

	@Override
	public String toString() {
		return "DataItemResult2{" +
				"key2=" + key2 +
				", metric1=" + metric1 +
				", metric2=" + metric2 +
				", metric3=" + metric3 +
				'}';
	}
}
