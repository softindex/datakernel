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

import java.util.Collections;
import java.util.List;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class ReadResponse<K, R extends AbstractRecord<K>> {
	private final List<R> records;
	private final int count;
	private List<R> extra;
	private R totals;

	private ReadResponse(List<R> records, int count, List<R> extra, R totals) {
		this.records = checkNotNull(records, "Records cannot be null in ReadResponse");
		this.count = count;
		this.extra = checkNotNull(extra, "Extras cannot be null in ReadResponse");
		this.totals = totals;
	}

	public static <K, R extends AbstractRecord<K>> ReadResponse<K, R> of(List<R> records, int count) {
		return new ReadResponse<>(records, count, Collections.<R>emptyList(), null);
	}

	public static <K, R extends AbstractRecord<K>> ReadResponse<K, R> of(List<R> records, int count, List<R> extra) {
		return new ReadResponse<>(records, count, extra, null);
	}

	public static <K, R extends AbstractRecord<K>> ReadResponse<K, R> of(List<R> records, int count, List<R> extra, R totals) {
		return new ReadResponse<>(records, count, extra, totals);
	}

	String toJson(Gson gson, Class<R> type, Class<K> idType) {
		JsonObject result = new JsonObject();

		JsonArray recs = new JsonArray();
		for (R record : records) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(record.getId(), idType));
			arr.add(gson.toJsonTree(record, type));
			recs.add(arr);
		}
		result.add("records", recs);

		JsonArray extras = new JsonArray();
		for (R record : extra) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(record.getId(), idType));
			arr.add(gson.toJsonTree(record, type));
			extras.add(arr);
		}
		result.add("extra", extras);

		if (totals != null) {
			result.add("total", gson.toJsonTree(totals, type));
		}

		result.add("count", gson.toJsonTree(count));
		return gson.toJson(result);
	}
}
