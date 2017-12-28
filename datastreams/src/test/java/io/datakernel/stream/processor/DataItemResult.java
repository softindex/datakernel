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

	@Override
	public boolean equals(Object o) {
		DataItemResult that = (DataItemResult) o;

		return (key1 == that.key1) && (key2 == that.key2)
				&& (metric1 == that.metric1) && (metric2 == that.metric2) && (metric3 == that.metric3);
	}

	@Override
	public String toString() {
		return "DataItemResult{" +
				"key1=" + key1 +
				", key2=" + key2 +
				", metric1=" + metric1 +
				", metric2=" + metric2 +
				", metric3=" + metric3 +
				'}';
	}
}
