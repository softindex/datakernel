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

import io.datakernel.aggregation.annotation.Key;
import io.datakernel.aggregation.annotation.Measures;

public class DataItem4 {
	@Key
	public int key1;
	@Key
	public int key2;
	@Key
	public int key3;
	@Key
	public int key4;
	@Key
	public int key5;

	@Measures
	public long metric2;
	@Measures
	public long metric3;

	public DataItem4() {
	}

	public DataItem4(int key1, int key2, int key3, int key4, int key5, long metric2, long metric3) {
		this.key1 = key1;
		this.key2 = key2;
		this.key3 = key3;
		this.key4 = key4;
		this.key5 = key5;
		this.metric2 = metric2;
		this.metric3 = metric3;
	}
}
