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

package io.datakernel.uikernel;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.List;

@SuppressWarnings("unused")
public final class ReadResponse<K, R extends AbstractRecord<K>> {
	private final List<R> records;
	private final int count;
	private List<R> extra;
	private R totals;

	public ReadResponse(List<R> records, int count) {
		this(records, count, null, null);
	}

	public ReadResponse(List<R> records, int count, List<R> extra, R totals) {
		// TODO (arashev): add preconditions and null checks, here and in other similar classes
		this.records = records;
		this.count = count;
		this.extra = extra;
		this.totals = totals;
	}

	public void setExtra(List<R> extra) {
		this.extra = extra;
	}

	public void setTotals(R totals) {
		this.totals = totals;
	}

	JsonObject toJson(Gson gson, Class<R> type, Class<K> idType) {
		JsonObject result = new JsonObject();

		JsonArray recs = new JsonArray();
		for (R record : records) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(record.getId(), idType));
			arr.add(gson.toJsonTree(record, type));
			recs.add(arr);
		}
		result.add("records", recs);

		if (extra != null) {
			JsonArray extras = new JsonArray();
			for (R record : extra) {
				JsonArray arr = new JsonArray();
				arr.add(gson.toJsonTree(record.getId(), idType));
				arr.add(gson.toJsonTree(record, type));
				extras.add(arr);
			}
			result.add("extra", extras);
		}

		if (totals != null) {
			result.add("total", gson.toJsonTree(totals, type));
		}

		result.add("count", gson.toJsonTree(count));
		return result;
	}
}