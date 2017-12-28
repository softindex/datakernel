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

package io.datakernel.stream.processor;

public class DataItemKey implements Comparable<DataItemKey> {
	public int key1;
	public int key2;

	public DataItemKey() {
	}

	public DataItemKey(int key1, int key2) {
		this.key1 = key1;
		this.key2 = key2;
	}

	@Override
	public boolean equals(Object o) {
		DataItemKey that = (DataItemKey) o;

		return key1 == that.key1 && key2 == that.key2;
	}

	@Override
	public String toString() {
		return "DataItemKey{" +
				"key1=" + key1 +
				", key2=" + key2 +
				'}';
	}

	@Override
	public int compareTo(DataItemKey o) {
		int result = Integer.compare(key1, o.key1);
		if (result != 0)
			return result;
		return Integer.compare(key2, o.key2);
	}

	@Override
	public int hashCode() {
		int result = key1;
		result = 31 * result + key2;
		return result;
	}
}
