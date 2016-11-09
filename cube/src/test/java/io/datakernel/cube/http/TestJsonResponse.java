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

package io.datakernel.cube.http;

import io.datakernel.cube.bean.DataItemResult;

public class TestJsonResponse {
	private final DataItemResult[] rows;
	private final int count;

	public TestJsonResponse(DataItemResult[] rows, int count) {
		this.rows = rows;
		this.count = count;
	}

	public DataItemResult[] getRows() {
		return rows;
	}

	public int getCount() {
		return count;
	}
}
