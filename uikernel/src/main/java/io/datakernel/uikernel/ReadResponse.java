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
public final class ReadResponse<E extends AbstractRecord<T>, T> {
	private final List<E> records;
	private List<E> extra;
	private E totals;
	private final int count;

	public ReadResponse(List<E> records, int count) {
		this(records, null, null, count);
	}

	public ReadResponse(List<E> records, List<E> extra, E totals, int count) {
		this.records = records;
		this.extra = extra;
		this.totals = totals;
		this.count = count;
	}

	public void setExtra(List<E> extra) {
		this.extra = extra;
	}

	public void setTotals(E totals) {
		this.totals = totals;
	}

	String toJson(Gson gson, Class<E> type, Class<T> idType) {
		JsonObject root = new JsonObject();

		JsonArray recs = new JsonArray();
		for (E record : records) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(record.getId(), idType));
			arr.add(gson.toJsonTree(record, type));
			recs.add(arr);
		}
		root.add("records", recs);

		JsonArray extras = new JsonArray();
		for (E record : extra) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(record.getId(), idType));
			arr.add(gson.toJsonTree(record, type));
			extras.add(arr);
		}
		root.add("extra", extras);

		root.add("total", gson.toJsonTree(totals, type));

		root.add("count", gson.toJsonTree(count));
		return gson.toJson(root);
	}
}