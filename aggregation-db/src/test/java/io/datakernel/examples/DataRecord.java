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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * First let's create a simple DataRecord class that holds a compound key that consists of two properties, and two values.
 */
public class DataRecord {
	public int key1;
	public long key2;

	public long value1;
	public double value2;

	public DataRecord() {
	}

	public DataRecord(int key1, long key2, long value1, double value2) {
		this.key1 = key1;
		this.key2 = key2;
		this.value1 = value1;
		this.value2 = value2;
	}

	public static final List<String> KEYS = asList("key1", "key2");

	public static final List<String> FIELDS = asList("value1", "value2");

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DataRecord that = (DataRecord) o;

		if (key1 != that.key1) return false;
		if (key2 != that.key2) return false;
		if (value1 != that.value1) return false;
		return Double.compare(that.value2, value2) == 0;

	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		result = key1;
		result = 31 * result + (int) (key2 ^ (key2 >>> 32));
		result = 31 * result + (int) (value1 ^ (value1 >>> 32));
		temp = Double.doubleToLongBits(value2);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("key1", key1)
				.add("key2", key2)
				.add("value1", value1)
				.add("value2", value2)
				.toString();
	}
}
