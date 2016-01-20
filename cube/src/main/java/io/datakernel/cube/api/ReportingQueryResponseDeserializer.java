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

package io.datakernel.cube.api;

import com.google.gson.*;
import io.datakernel.aggregation_db.AggregationStructure;
import io.datakernel.aggregation_db.keytype.KeyType;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportingQueryResponseDeserializer implements JsonDeserializer<ReportingQueryResult> {
	private final AggregationStructure structure;

	public ReportingQueryResponseDeserializer(AggregationStructure structure) {
		this.structure = structure;
	}

	@Override
	public ReportingQueryResult deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext ctx)
			throws JsonParseException {
		JsonObject json = jsonElement.getAsJsonObject();

		int count = json.get("count").getAsInt();

		JsonArray jsonRecords = json.get("records").getAsJsonArray();
		List<Map<String, Object>> records = new ArrayList<>(count);
		for (JsonElement jsonRecordElement : jsonRecords) {
			JsonObject jsonRecord = jsonRecordElement.getAsJsonObject();

			Map<String, Object> record = new HashMap<>();
			for (Map.Entry<String, JsonElement> jsonRecordEntry : jsonRecord.entrySet()) {
				KeyType keyType = structure.getKeyType(jsonRecordEntry.getKey());
				if (keyType != null) {
					record.put(jsonRecordEntry.getKey(), keyType.fromJson(jsonRecordEntry.getValue()));
				} else if (structure.containsOutputField(jsonRecordEntry.getKey())) {
					record.put(jsonRecordEntry.getKey(), jsonRecordEntry.getValue().getAsNumber());
				} else
					throw new JsonParseException("Unknown record key/field");
			}
			records.add(record);
		}

		JsonObject jsonTotals = json.get("totals").getAsJsonObject();
		Map<String, Object> totals = new HashMap<>();
		for (Map.Entry<String, JsonElement> totalsEntry : jsonTotals.entrySet()) {
			totals.put(totalsEntry.getKey(), totalsEntry.getValue().getAsNumber());
		}

		return new ReportingQueryResult(records, totals, count);
	}
}
