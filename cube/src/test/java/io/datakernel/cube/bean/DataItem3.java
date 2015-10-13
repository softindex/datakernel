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

public class DataItem3 {
	public int key1;
	public int key2;
	public int key3;
	public int key4;
	public int key5;

	public long metric1;
	public long metric2;

	public DataItem3() {
	}

	public DataItem3(int key1, int key2, int key3, int key4, int key5, long metric1, long metric2) {
		this.key1 = key1;
		this.key2 = key2;
		this.key3 = key3;
		this.key4 = key4;
		this.key5 = key5;
		this.metric1 = metric1;
		this.metric2 = metric2;
	}

	public static final List<String> DIMENSIONS = asList("key1", "key2", "key3", "key4", "key5");

	public static final List<String> METRICS = asList("metric1", "metric2");
}
