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

import java.util.List;

import static java.util.Arrays.asList;

public class DataItemString1 {
	public String key1;
	public int key2;

	public long metric1;
	public long metric2;

	public DataItemString1() {
	}

	public DataItemString1(String key1, int key2, long metric1, long metric2) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric1 = metric1;
		this.metric2 = metric2;
	}

	public static final List<String> DIMENSIONS = asList("key1", "key2");

	public static final List<String> METRICS = asList("metric1", "metric2");

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataItemString1 that = (DataItemString1) o;

		if (key2 != that.key2) return false;
		if (metric1 != that.metric1) return false;
		if (metric2 != that.metric2) return false;
		return !(key1 != null ? !key1.equals(that.key1) : that.key1 != null);

	}

	@Override
	public int hashCode() {
		int result = key1 != null ? key1.hashCode() : 0;
		result = 31 * result + key2;
		result = 31 * result + (int) (metric1 ^ (metric1 >>> 32));
		result = 31 * result + (int) (metric2 ^ (metric2 >>> 32));
		return result;
	}
}
