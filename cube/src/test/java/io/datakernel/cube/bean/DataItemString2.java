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

import java.util.List;

import static java.util.Arrays.asList;

public class DataItemString2 {
	public String key1;
	public int key2;

	public long metric2;
	public long metric3;

	public DataItemString2() {
	}

	public DataItemString2(String key1, int key2, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}

	public static final List<String> DIMENSIONS = asList("key1", "key2");

	public static final List<String> METRICS = asList("metric2", "metric3");

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataItemString2 that = (DataItemString2) o;

		if (key2 != that.key2) return false;
		if (metric2 != that.metric2) return false;
		if (metric3 != that.metric3) return false;
		return !(key1 != null ? !key1.equals(that.key1) : that.key1 != null);

	}

	@Override
	public int hashCode() {
		int result = key1 != null ? key1.hashCode() : 0;
		result = 31 * result + key2;
		result = 31 * result + (int) (metric2 ^ (metric2 >>> 32));
		result = 31 * result + (int) (metric3 ^ (metric3 >>> 32));
		return result;
	}
}
